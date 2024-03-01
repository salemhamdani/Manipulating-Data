package spark.kafka;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;

import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class SparkKafkaAccommodation {

    // take a whole line in the csv dataset as an input
    private static final Pattern SPLITTER = Pattern.compile(",");

    private SparkKafkaAccommodation() {

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SparkKafkaWordCount <zkQuorum> <group> <topics> <numThreads> ");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaAccommodation");

        // Connect to MongoDB using Mongoose
        String connectionString = "mongodb+srv://salemhamdani:yTMKayeL6x3cJAxM@cluster0.zzyw6fa.mongodb.net/?retryWrites=true&w=majority";


        // Create le context of length de batch of 2 seconds
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<Tuple2<String, Double>> pairs = lines.map(line -> {
            String[] fields = line.split(String.valueOf(SPLITTER));
            if (fields.length == 2) {
                String name = fields[0];
                try {
                    Double price = Double.parseDouble(fields[1]);

                    MongoClient mongoClient = MongoClients.create(connectionString);
                    MongoDatabase database = mongoClient.getDatabase("airbnb");
                    // Get a collection to store documents in
                    MongoCollection<Document> collection = database.getCollection("houses");
                    // Create a new document to store in MongoDB
                    Document doc = new Document();
                    doc.append("city", name);
                    doc.append("price", price);
                    doc.append("show", true);
                    doc.append("date", new Date().toString());

                    // Insert the document into the collection
                    collection.insertOne(doc);

                    return new Tuple2<>(name, price);
                } catch (NumberFormatException e) {
                    // The price is not a valid double value
                    return null;
                }
            } else {
                // The line is not in the correct format
                return null;
            }
        }).filter(Objects::nonNull);

        JavaPairDStream<String, Double> prices = pairs.mapToPair(pair -> new Tuple2<>(pair._1(), pair._2()));

        JavaPairDStream<String, Double> totalPrices = prices.reduceByKey(Double::sum);

        totalPrices.print();
        jssc.start();
        jssc.awaitTermination();

    }

}
