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
import java.util.function.Consumer;

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
            newStudent.append("Courses", line[2]);
            students.add(newStudent);
        });

        // Adding all object to database
        newCollection.insertMany(students);

        long collectionSize = newCollection.countDocuments();

        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
//        collection.find(query).forEach((Consumer<Document>) document -> {
//            System.out.println("Наш второй документ:\n" + document);
//        });
        List<Consumer<Document>> olderThan40 = new ArrayList<>();
       // newCollection.find(query).forEach((Consumer<Document>) doc -> System.out.println(doc.get("age")));

        newCollection.find(query).forEach((Consumer<Document>) doc -> {
            olderThan40.add((Consumer<Document>) doc);
        });

        System.out.println(olderThan40.size());

        //  System.out.println(newCollection.count());
       // System.out.println(newRes.size());


//        newCollection.find().forEach((Consumer<Document>) record -> {
//            System.out.println(record.toString());
//        });


//        // Создадим первый документ
//        Document firstDocument = new Document()
//                .append("Type", 1)
//                .append("Description", "Это наш первый документ в MongoDB")
//                .append("Author", "Я")
//                .append("Time", new SimpleDateFormat().format(new Date()));
//
//
//        // Вложенный объект
//        Document nestedObject = new Document()
//                .append("Course", "NoSQL Базы Данных")
//                .append("Author", "Mike Ovchinnikov");
//
//        firstDocument.append("Skillbox", nestedObject);
//
//
//        // Вставляем документ в коллекцию
//        collection.insertOne(firstDocument);

//        newCollection.find().forEach((Consumer<Document>) document -> {
//            System.out.println(document);
//        });
////
//        // Используем JSON-синтаксис для создания объекта
//        Document secondDocument = Document.parse(
//                "{Type: 2, Description:\"Мы создали и нашли этот документ с помощью JSON-синтаксиса\"}"
//        );
//        collection.insertOne(secondDocument);
//
//        // Используем JSON-синтаксис для написания запроса (выбираем документы с Type=2)
//        BsonDocument query = BsonDocument.parse("{Type: {$eq: 2}}");
//        collection.find(query).forEach((Consumer<Document>) document -> {
//            System.out.println("Наш второй документ:\n" + document);
//        });
    }
}
