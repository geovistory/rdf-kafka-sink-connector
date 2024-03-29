package org.geovistory.kafka.sink.connector.rdf.fuseki;

import org.apache.jena.atlas.logging.Log;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownServiceException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class DatasetHandler {
    /**
     * @param datasetName           the name of the dataset
     * @param fusekiUrl             the URL of the fuseki instance
     * @param httpHeadersAuthConfig auth config to log into Fuseki
     * @return response code of the request
     * @throws IOException IOexception
     */
    public static int createFusekiDataset(String datasetName, String fusekiUrl, String httpHeadersAuthConfig) throws IOException {
        System.out.println("createFusekiDataset  " + datasetName + "...");

        String template = prepareTemplate(datasetName);
        String url = fusekiUrl + "/$/datasets";
        String base64 = Base64.getEncoder().encodeToString(httpHeadersAuthConfig.getBytes(StandardCharsets.UTF_8));

        byte[] blob = Files.readAllBytes(Paths.get(template));

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "text/turtle");
        connection.setRequestProperty("Authorization", "Basic " + base64);
        //connection.setRequestProperty("Authorization", "Basic YWRtaW46cHc=");

        try {
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(blob);
        } catch (IOException e) {
            Log.error(e, e.getMessage());
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

        return responseCode;
    }

    private static String prepareTemplate(String datasetName) throws IOException {
        InputStream inputStream = DatasetHandler.class.getResourceAsStream("/datasetTemplate.ttl");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder stringBuilder = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            line = line.replace("my_dataset", datasetName);
            stringBuilder.append(line).append("\n");
        }

        reader.close();

        String templateFilePath = "/tmp/kafka/template-" + datasetName;
        File outputFile = new File(templateFilePath);
        File parentDir = outputFile.getParentFile();

        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new IOException("Failed to create parent directory: " + parentDir.getAbsolutePath());
            }
        }

        if (!outputFile.exists()) {
            if (!outputFile.createNewFile()) {
                throw new IOException("Failed to create output file: " + outputFile.getAbsolutePath());
            }
        }

        outputFile.setWritable(true);

        // Write the modified template to the output file
        FileWriter writer = new FileWriter(outputFile);
        writer.write(stringBuilder.toString());
        writer.close();

        return templateFilePath;
    }

}
