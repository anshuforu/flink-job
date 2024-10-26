package com.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

public class FlinkTemperatureAlert {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "b-1.democluster1.kafka.us-east-1.amazonaws.com:9092,b-2.democluster1.kafka.us-east-1.amazonaws.com:9092,b-3.democluster1.kafka.us-east-1.amazonaws.com:9092");
        properties.setProperty("group.id", "flink-temperature-alert");
        properties.setProperty("acks", "all");  // Ensure reliable message delivery for Kafka producer

        // Kafka consumer for 'iot-sensor-topic'
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "iot-sensor-topic",            // Kafka topic for sensor data
                new SimpleStringSchema(),       // Schema for reading string data
                properties
        );

        // Stream of raw sensor data from Kafka
        DataStream<String> sensorStream = env.addSource(consumer);

        // Process and parse the sensor data with error handling
        DataStream<Tuple2<String, Double>> parsedStream = sensorStream
                .map(sensorData -> {
                    try {
                        // Assume sensor data is in format: "device_id,temperature"
                        String[] parts = sensorData.split(",");
                        String deviceId = parts[0];
                        double temperature = Double.parseDouble(parts[1]);
                        return new Tuple2<>(deviceId, temperature);
                    } catch (Exception e) {
                        return new Tuple2<>("invalid", -999.0);  // Handle malformed data
                    }
                });

        // Aggregate temperature over 1 minute windows
        DataStream<Tuple2<String, Double>> aggregatedTemperature = parsedStream
                .keyBy(sensor -> sensor.f0)   // Key by device_id (first field)
                .timeWindow(Time.minutes(1))  // 1-minute window
                .aggregate(new TemperatureAggregator());

        // Check for temperature anomalies (if > 50)
        DataStream<String> alertStream = aggregatedTemperature
                .filter(sensor -> sensor.f1 > 50)  // Check if temperature > 50
                .map(sensor -> "ALERT: Device " + sensor.f0 + " has high temperature: " + sensor.f1 + "°C");

        // Kafka producer to send alerts to another Kafka topic
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "iot-sensor-alerts",          // Kafka topic for alerts
                new SimpleStringSchema(),      // Schema for sending string data
                properties
        );

        // Send the alert stream to the Kafka topic
        alertStream.addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("Flink Temperature Alert Job");
    }

    // Aggregator to compute average temperature
    public static class TemperatureAggregator implements AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return new Tuple3<>("", 0.0, 0); // Accumulate deviceId, sum of temperatures, and count
        }

        @Override
        public Tuple3<String, Double, Integer> add(Tuple2<String, Double> value, Tuple3<String, Double, Integer> accumulator) {
            return new Tuple3<>(value.f0, accumulator.f1 + value.f1, accumulator.f2 + 1);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0, accumulator.f1 / accumulator.f2); // Compute average temperature
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
            return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
