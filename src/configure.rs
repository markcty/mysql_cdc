use crate::binlog_client::BinlogClient;
use crate::commands::query_command::QueryCommand;
use crate::constants::checksum_type::ChecksumType;
use crate::errors::Error;
use crate::extensions::check_error_packet;
use crate::packet_channel::PacketChannel;
use crate::responses::response_type;
use crate::responses::result_set_row_packet::ResultSetRowPacket;
use crate::starting_strategy::StartingStrategy;

impl BinlogClient {
    pub async fn adjust_starting_position(
        &mut self,
        channel: &mut PacketChannel,
    ) -> Result<(), Error> {
        if self.options.binlog.starting_strategy != StartingStrategy::FromEnd {
            return Ok(());
        }

        // Ignore if position was read before in case of reconnect.
        if !self.options.binlog.filename.is_empty() {
            return Ok(());
        }

        let command = QueryCommand::new("show master status".to_string());
        channel.write_packet(&command.serialize()?, 0).await?;

        let result_set = self.read_result_set(channel).await?;
        if result_set.len() != 1 {
            return Err(Error::String(
                "Could not read master binlog position.".to_string(),
            ));
        }

        self.options.binlog.filename = result_set[0].cells[0].clone();
        self.options.binlog.position = result_set[0].cells[1].parse()?;
        Ok(())
    }

    pub async fn set_master_heartbeat(&mut self, channel: &mut PacketChannel) -> Result<(), Error> {
        let milliseconds = self.options.heartbeat_interval.as_millis();
        let nanoseconds = milliseconds * 1000 * 1000;
        let query = format!("set @master_heartbeat_period={}", nanoseconds);
        let command = QueryCommand::new(query.to_string());
        channel.write_packet(&command.serialize()?, 0).await?;
        let (packet, _) = channel.read_packet().await?;
        check_error_packet(&packet, "Setting master heartbeat error.")?;
        Ok(())
    }

    pub async fn set_master_binlog_checksum(
        &mut self,
        channel: &mut PacketChannel,
    ) -> Result<ChecksumType, Error> {
        let command =
            QueryCommand::new("SET @master_binlog_checksum= @@global.binlog_checksum".to_string());
        channel.write_packet(&command.serialize()?, 0).await?;
        let (packet, _) = channel.read_packet().await?;
        check_error_packet(&packet, "Setting master_binlog_checksum error.")?;

        let command = QueryCommand::new("SELECT @master_binlog_checksum".to_string());
        channel.write_packet(&command.serialize()?, 0).await?;
        let result_set = self.read_result_set(channel).await?;

        // When replication is started fake RotateEvent comes before FormatDescriptionEvent.
        // In order to deserialize the event we have to obtain checksum type length in advance.
        ChecksumType::from_name(&result_set[0].cells[0])
    }

    async fn read_result_set(
        &self,
        channel: &mut PacketChannel,
    ) -> Result<Vec<ResultSetRowPacket>, Error> {
        let (packet, _) = channel.read_packet().await?;
        check_error_packet(&packet, "Reading result set error.")?;

        loop {
            // Skip through metadata
            let (packet, _) = channel.read_packet().await?;
            if packet[0] == response_type::END_OF_FILE {
                break;
            }
        }

        let mut result_set = Vec::new();
        loop {
            let (packet, _) = channel.read_packet().await?;
            check_error_packet(&packet, "Query result set error.")?;
            if packet[0] == response_type::END_OF_FILE {
                break;
            }
            result_set.push(ResultSetRowPacket::parse(&packet)?);
        }
        Ok(result_set)
    }
}
