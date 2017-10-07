package com.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class MongoSampleClient extends DefaultHandler {

	private String temp;
	private String[] words;
	private String wordKey = "word";
	private boolean flag = true;
	private int freq = 0;
	private MongoClient mongoClient = null;
	private MongoDatabase db = null;
	private MongoCollection<Document> table = null;

	public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
		MongoSampleClient handler = new MongoSampleClient();
		String file = args[0];
		String address[] = args[1].split(":");
		int port = Integer.parseInt(address[1]);
		handler.mongoClient = new MongoClient(address[0], port);
		handler.db = handler.mongoClient.getDatabase("wordcount");
		SAXParserFactory spfac = SAXParserFactory.newInstance();
		SAXParser sp = spfac.newSAXParser();
		sp.parse(file, handler);
		handler.minFrequency();
		handler.maxFrequency();

	}

	public void characters(char[] buffer, int start, int length) {
		temp = new String(buffer, start, length);
		frequencyCount(temp.trim());
	}

	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
	}

	public void endElement(String uri, String localName, String qName) throws SAXException {

	}

	private void frequencyCount(String temp) {
		temp = temp.replaceAll("[^a-zA-Z0-9]", " ");
		words = temp.split(" ");
		for (String s : words) {
			String a = s.trim();
			calldb(a);

		}
	}

	public void calldb(String s) {
		try {
			table = db.getCollection("w9");

			BasicDBObject fields = new BasicDBObject();
			List<BasicDBObject> searchArguments = new ArrayList<BasicDBObject>();
			searchArguments.add(new BasicDBObject(s.toLowerCase(), new BasicDBObject("$eq", 1)));
			searchArguments.add(new BasicDBObject(s.toLowerCase(), new BasicDBObject("$gt", 1)));

			fields.put("$or", searchArguments);

			FindIterable<Document> it = table.find(fields);
			MongoCursor<Document> iterator = it.iterator();

			while (iterator.hasNext()) {
				freq = iterator.next().getInteger(s.toLowerCase());
				if (freq > 0) {
					flag = false;
					Bson filter = Filters.exists(s.toLowerCase(), true);
					Document doc1 = new Document(wordKey, freq + 1).append(s.toLowerCase(), freq + 1);
					table.findOneAndReplace(filter, doc1);

				}

			}
			if (!s.equals("") && flag) {
				Document doc = new Document(wordKey, 1);
				doc.append(s.toLowerCase(), 1);
				table.insertOne(doc);

			}
			flag = true;
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void minFrequency() {
		BsonValue b = new BsonInt32(+1);
		Bson sort = new BsonDocument(wordKey, b);
		Document min = table.find().sort(sort).first();
		Map<String, Integer> minFreq = getWord(min);
		minFreq.forEach((key, value) -> System.out.println("least freaquency : " + key + " : " + value));
	}

	public void maxFrequency() {
		BsonValue b = new BsonInt32(-1);
		Bson sort = new BsonDocument(wordKey, b);
		Document max = table.find().sort(sort).first();
		Map<String, Integer> maxFreq = getWord(max);
		maxFreq.forEach((key, value) -> System.out.println("Max freaquency : " + key + " : " + value));
	}

	public Map<String, Integer> getWord(Document document) {
		Map<String, Integer> count = new HashMap<>();
		Iterator<String> key = document.keySet().iterator();
		while (key.hasNext()) {
			String wk = key.next();
			if (!(wk.equals(wordKey) || wk.equals("_id")))
				count.put(wk, document.getInteger(wk));
		}
		return count;
	}

}