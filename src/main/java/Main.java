import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static String infile = "src/main/data/mongo.csv";
    private static String collection = "parsedCSV";
    private static String databaseName = "local";
    private static String host = "127.0.0.1";
    private static int port = 27017;

    public static void main(String[] args) throws IOException {

        // Parsing CSV file
        CSVReader reader = new CSVReader(new FileReader(infile) , ',' , '"','\\');
        List<String[]> linesFromFile = reader.readAll();
        for (int i = 0; i < linesFromFile.size() ; i++) {
            if (linesFromFile.get(i).length != 3) {
                linesFromFile.remove(linesFromFile.get(i));
            }
        }

        // Connecting to MongoDB, creating collection
        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> newCollection = database.getCollection(collection);
        newCollection.drop();

        // Creating objects for collection
        List<Document> students = new ArrayList<>();
        linesFromFile.forEach(line -> {
            Document newStudent = new Document();
            newStudent.append("name", line[0]);
            newStudent.append("age", Integer.parseInt(line[1]));
            newStudent.append("courses", line[2]);
            students.add(newStudent);
        });

        // Adding all object to database
        newCollection.insertMany(students);

        // Printing collection size
        long collectionSize = newCollection.countDocuments();
        System.out.println("Total students: " + collectionSize);

        // Getting and printing count of students, older than 40
        BsonDocument queryFour = BsonDocument.parse("{age: {$gt: 40}}");
        Iterable<Document> count = newCollection.find(queryFour);
        AtomicInteger i = new AtomicInteger();
        count.forEach(document -> i.getAndIncrement());
        System.out.println("Students, older than 40: " + i);


        // Getting and printing yangest student name
        BsonDocument queryYangest = BsonDocument.parse("{age: 1}");
        Iterable<Document> yangestStudent = newCollection.find().sort(queryYangest).limit(1);
        yangestStudent.forEach(student -> System.out.println("Yangest student name is: " + student.get("name")));

        //Getting and printing oldest student courses
        BsonDocument queryOldest = BsonDocument.parse("{age: -1}");
        Iterable<Document> oldestStudent = newCollection.find().sort(queryOldest).limit(1);
        oldestStudent.forEach(student -> System.out.println("Oldest student courses are: " + student.get("courses")));

    }
}
