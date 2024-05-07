/*
 * MIT License
 *
 * Copyright (c) [2022] [Ondrej Babec <ond.babec@gmail.com>]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

use embedded_io::ReadReady;
use embedded_io_async::{Read, Write};
use heapless::Vec;
use rand_core::RngCore;

use crate::client::client_config::ClientConfig;
use crate::packet::v5::publish_packet::QualityOfService::{self, QoS1};
use crate::packet::v5::reason_codes::ReasonCode;

use super::raw_client::{Event, RawMqttClient};

pub struct MqttClient<'a, T, const MAX_PROPERTIES: usize, const MAX_TOPICS: usize, R: RngCore>
where
    T: Read + Write + ReadReady,
{
    raw: RawMqttClient<'a, T, MAX_PROPERTIES, R>,
    act_identifiers: Vec<u16, 10>,
}

impl<'a, T, const MAX_PROPERTIES: usize, const MAX_TOPICS: usize, R> MqttClient<'a, T, MAX_PROPERTIES, MAX_TOPICS, R>
where
    T: Read + Write + ReadReady,
    R: RngCore,
{
    pub fn new(
        network_driver: T,
        buffer: &'a mut [u8],
        buffer_len: usize,
        recv_buffer: &'a mut [u8],
        recv_buffer_len: usize,
        config: ClientConfig<'a, MAX_PROPERTIES, R>,
    ) -> Self {
        Self {
            raw: RawMqttClient::new(
                network_driver,
                buffer,
                buffer_len,
                recv_buffer,
                recv_buffer_len,
                config,
            ),
            act_identifiers: Vec::new()
        }
    }

    /// Method allows client connect to server. Client is connecting to the specified broker
    /// in the `ClientConfig`. Method selects proper implementation of the MQTT version based on the config.
    /// If the connection to the broker fails, method returns Err variable that contains
    /// Reason codes returned from the broker.
    pub async fn connect_to_broker(&mut self) -> Result<(), ReasonCode> {
        self.raw.connect_to_broker().await?;

        match self.raw.poll::<0>().await? {
            Event::Connack => Ok(()),
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }

    /// Method allows client disconnect from the server. Client disconnects from the specified broker
    /// in the `ClientConfig`. Method selects proper implementation of the MQTT version based on the config.
    /// If the disconnect from the broker fails, method returns Err variable that contains
    /// Reason codes returned from the broker.
    pub async fn disconnect(&mut self) -> Result<(), ReasonCode> {
        self.raw.disconnect().await?;
        Ok(())
    }

    /// Method allows sending message to broker specified from the ClientConfig. Client sends the
    /// message from the parameter `message` to the topic `topic_name` on the broker
    /// specified in the ClientConfig. If the send fails method returns Err with reason code
    /// received by broker.
    pub async fn send_message(
        &mut self,
        topic_name: &str,
        message: &[u8],
        qos: QualityOfService,
        retain: bool,
    ) -> Result<(), ReasonCode> {
        let identifier = self
            .raw
            .send_message(topic_name, message, qos, retain)
            .await?;

        // QoS1
        if qos == QoS1 {
            match self.raw.poll::<0>().await? {
                Event::Puback(ack_identifier) => {
                    if identifier == ack_identifier {
                        Ok(())
                    } else {
                        Err(ReasonCode::PacketIdentifierNotFound)
                    }
                }
                Event::Disconnect(reason) => Err(reason),
                // If an application message comes at this moment, it is lost.
                _ => Err(ReasonCode::ImplementationSpecificError),
            }
        } else {
            Ok(())
        }
    }

    /// Method allows sending message to broker specified from the ClientConfig. Client sends the
    /// message from the parameter `message` to the topic `topic_name` on the broker
    /// specified in the ClientConfig. If the send fails method returns Err with reason code
    /// received by broker.
    pub async fn send_message_no_block(
        &mut self,
        topic_name: &str,
        message: &[u8],
        qos: QualityOfService,
        retain: bool,
    ) -> Result<(), ReasonCode> {
        let identifier = self
            .raw
            .send_message(topic_name, message, qos, retain)
            .await?;
        if qos == QoS1 {
            if let Err(_) = self.act_identifiers.push(identifier) {
                return Err(ReasonCode::ImplementationSpecificError);
            }
        }
        Ok(())
    }

    /// Method allows client subscribe to multiple topics specified in the parameter
    /// `topic_names` on the broker specified in the `ClientConfig`. Generics `TOPICS`
    /// sets the value of the `topics_names` vector. MQTT protocol implementation
    /// is selected automatically.
    pub async fn subscribe_to_topics(
        &mut self,
        topic_names: &Vec<&str, MAX_TOPICS>,
    ) -> Result<(), ReasonCode> {
        let identifier = self.raw.subscribe_to_topics(topic_names).await?;

        match self.raw.poll::<MAX_TOPICS>().await? {
            Event::Suback(ack_identifier) => {
                if identifier == ack_identifier {
                    Ok(())
                } else {
                    Err(ReasonCode::PacketIdentifierNotFound)
                }
            }
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }

    /// Method allows client subscribe to multiple topics specified in the parameter
    /// `topic_names` on the broker specified in the `ClientConfig`. Generics `TOPICS`
    /// sets the value of the `topics_names` vector. MQTT protocol implementation
    /// is selected automatically.
    pub async fn subscribe_to_topics_no_block(
        &mut self,
        topic_names: &Vec<&str, MAX_TOPICS>,
    ) -> Result<(), ReasonCode> {
        let identifier = self.raw.subscribe_to_topics(topic_names).await?;
        if let Err(_) = self.act_identifiers.push(identifier) {
            return Err(ReasonCode::ImplementationSpecificError);
        }
        Ok(())
    }

    /// Method allows client unsubscribe from the topic specified in the parameter
    /// `topic_name` on the broker from the `ClientConfig`. MQTT protocol implementation
    /// is selected automatically.
    pub async fn unsubscribe_from_topic(
        &mut self,
        topic_name: &str,
    ) -> Result<(), ReasonCode> {
        let identifier = self.raw.unsubscribe_from_topic(topic_name).await?;

        match self.raw.poll::<0>().await? {
            Event::Unsuback(ack_identifier) => {
                if identifier == ack_identifier {
                    Ok(())
                } else {
                    Err(ReasonCode::PacketIdentifierNotFound)
                }
            }
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }

    /// Method allows client unsubscribe from the topic specified in the parameter
    /// `topic_name` on the broker from the `ClientConfig`. MQTT protocol implementation
    /// is selected automatically.
    pub async fn unsubscribe_from_topic_no_block(
        &mut self,
        topic_name: &str,
    ) -> Result<(), ReasonCode> {
        let identifier = self.raw.unsubscribe_from_topic(topic_name).await?;
        if let Err(_) = self.act_identifiers.push(identifier) {
            return Err(ReasonCode::ImplementationSpecificError);
        }
        Ok(())
    }

    /// Method allows client subscribe to multiple topics specified in the parameter
    /// `topic_name` on the broker specified in the `ClientConfig`. MQTT protocol implementation
    /// is selected automatically.
    pub async fn subscribe_to_topic(
        &mut self,
        topic_name: &str,
    ) -> Result<(), ReasonCode> {
        let mut topic_names = Vec::<&str, 1>::new();
        topic_names.push(topic_name).unwrap();

        let identifier = self.raw.subscribe_to_topics(&topic_names).await?;

        match self.raw.poll::<1>().await? {
            Event::Suback(ack_identifier) => {
                if identifier == ack_identifier {
                    Ok(())
                } else {
                    Err(ReasonCode::PacketIdentifierNotFound)
                }
            }
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }

    /// Method allows client subscribe to multiple topics specified in the parameter
    /// `topic_name` on the broker specified in the `ClientConfig`. MQTT protocol implementation
    /// is selected automatically.
    pub async fn subscribe_to_topic_no_block(
        &mut self,
        topic_name: &str,
    ) -> Result<(), ReasonCode> {
        let mut topic_names = Vec::<&str, 1>::new();
        topic_names.push(topic_name).unwrap();

        let identifier = self.raw.subscribe_to_topics(&topic_names).await?;
        if let Err(_) = self.act_identifiers.push(identifier) {
            return Err(ReasonCode::ImplementationSpecificError);
        }
        Ok(())
    }

    /// Method allows client receive a message. The work of this method strictly depends on the
    /// network implementation passed in the `ClientConfig`. It expects the PUBLISH packet
    /// from the broker.
    pub async fn receive_message(&mut self) -> Result<(&str, &[u8]), ReasonCode> {
        match self.raw.poll::<0>().await? {
            Event::Message(topic, payload) => Ok((topic, payload)),
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }

    /// Method allows client send PING message to the broker specified in the `ClientConfig`.
    /// If there is expectation for long running connection. Method should be executed
    /// regularly by the timer that counts down the session expiry interval.
    pub async fn send_ping(&mut self) -> Result<(), ReasonCode> {
        self.raw.send_ping().await?;

        match self.raw.poll::<0>().await? {
            Event::Pingresp => Ok(()),
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }

    /// Method allows client send PING message to the broker specified in the `ClientConfig`.
    /// If there is expectation for long running connection. Method should be executed
    /// regularly by the timer that counts down the session expiry interval.
    pub async fn send_ping_no_block(&mut self) -> Result<(), ReasonCode> {
        self.raw.send_ping().await
    }

    pub async fn loop_no_block(&mut self) -> Result<Option<(&str, &[u8])>, ReasonCode> {
        if !self.raw.ready_to_pool() {
            return Ok(None);
        }
        match self.raw.poll::<MAX_TOPICS>().await? {
            Event::Puback(ack_identifier) 
            | Event::Suback(ack_identifier)
            | Event::Unsuback(ack_identifier) => {
                match self.act_identifiers.iter().position(|x| *x == ack_identifier) {
                    Some(i) => {
                        self.act_identifiers.remove(i);
                        Ok(None)
                    },
                    None => Err(ReasonCode::PacketIdentifierNotFound),
                }
            }
            Event::Pingresp => Ok(None),
            Event::Message(topic, payload) => Ok(Some((topic, payload))),
            Event::Disconnect(reason) => Err(reason),
            // If an application message comes at this moment, it is lost.
            _ => Err(ReasonCode::ImplementationSpecificError),
        }
    }
}
