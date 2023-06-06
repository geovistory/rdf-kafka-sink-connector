package org.geovistory.kafka.sink.connector.rdf.fuseki;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DatasetHandler {
    public static void createFusekiDataset(String datasetName, String fusekiUrl, String httpHeadersAuthConfig) throws IOException {
        System.out.println("createFusekiDataset  " + datasetName + "...");

        String template = prepareTemplate(datasetName);
        String mimetype = "text/plain";
        byte[] blob = template.getBytes(StandardCharsets.UTF_8);
        String url = fusekiUrl + "/$/datasets";
        String base64 = "";

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "text/turtle");

        base64 = Base64.getEncoder().encodeToString(httpHeadersAuthConfig.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + base64);

        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(blob);
        }

        int responseCode = connection.getResponseCode();
        String responseStatusText = connection.getResponseMessage();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            System.out.println(responseStatusText);
        } else if (responseStatusText.equals("Conflict")) {
            System.out.println("Dataset already exists");
        } else {
            throw new IOException(responseStatusText);
        }
    }

    private static String prepareTemplate(String datasetName) throws IOException {
        File templateFile = new File("src/main/resources/datasetTemplate.ttl");
        BufferedReader reader = new BufferedReader(new FileReader(templateFile));
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            line = line.replace("my_dataset", datasetName);
            stringBuilder.append(line).append("\n");
        }

        reader.close();

        String result = stringBuilder.toString();
        String templateFilePath = "./tmp/template-" + datasetName;
        File outputFile = new File(templateFilePath);
        outputFile.getParentFile().mkdirs();
        outputFile.createNewFile();
        outputFile.setWritable(true);

        // Write the modified template to the output file
        FileWriter writer = new FileWriter(outputFile);
        writer.write(result);
        writer.close();

        return templateFilePath;
    }
}
