package org.geovistory.kafka.sink.connector.rdf.fuseki;

import okhttp3.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class DatasetHandler {
    /**
     * @param datasetName
     * @param fusekiUrl
     * @param httpHeadersAuthConfig
     * @return response code of the request
     * @throws IOException
     */
    public static int createFusekiDataset(String datasetName, String fusekiUrl, String httpHeadersAuthConfig) throws IOException {
        System.out.println("createFusekiDataset  " + datasetName + "...");

        String template = prepareTemplate(datasetName);
        String url = fusekiUrl + "/$/datasets";
        String base64 = Base64.getEncoder().encodeToString(httpHeadersAuthConfig.getBytes(StandardCharsets.UTF_8));

        String mimetype = "text/plain";
        byte[] blob = Files.readAllBytes(Paths.get(template));

        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "text/turtle");
        connection.setRequestProperty("Authorization", "Basic " + base64);

        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(blob);
        }

        int responseCode = connection.getResponseCode();
        String responseStatusText = connection.getResponseMessage();

/*
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("text/plain");
        RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
                .addFormDataPart(datasetName, datasetName + ".ttl",
                        RequestBody.create(MediaType.parse("application/octet-stream"),
                                new File(template)))
                .build();
        Request request = new Request.Builder()
                .url(fusekiUrl)
                .method("POST", body)
                .addHeader("Authorization", "Basic " + base64)
                .build();
        Response response = client.newCall(request).execute();

        var responseCode = response.code();
        var responseStatusText = response.message();
        */
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
