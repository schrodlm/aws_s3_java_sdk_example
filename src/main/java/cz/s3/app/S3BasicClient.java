package cz.s3.app;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * 
 * Hello world!
 *
 */
public class S3BasicClient {
    public static void main(String[] args) throws IOException {
        // Cinfigure the endpoint
        URI endpointOverride = URI.create("https://compat.objectstorage.eu-frankfurt-1.oraclecloud.com/susr-rpo");

        // Create a new S3 client with the custom endpoint
        S3Client s3 = S3Client.builder()
                .region(Region.AF_SOUTH_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpointOverride)
                .build();

        // Specify the bucket and object key
        String bucket = "frkqbrydxwdp";

        // Prepare a target directory
        Path tagetDirectory = Paths.get("batches/");

        if (!Files.exists(tagetDirectory)) {
            Files.createDirectories(tagetDirectory);
        }

        // Check if directories to pass the downloaded object to exist
        File destDirInitBatches = new File(tagetDirectory + "/batch-daily");

        if (!destDirInitBatches.exists()) {
            if (!destDirInitBatches.mkdirs())
                throw new RuntimeException();
        }

        File destDirActualBatches = new File(tagetDirectory + "/batch-init");
        if (!destDirActualBatches.exists()) {
            if (!destDirActualBatches.mkdirs())
                throw new RuntimeException();
        }

        // ============================================================================
        
        // Create a ListObjectsV2Request object
        ListObjectsV2Request listObjectsReqManual = ListObjectsV2Request.builder()
                .bucket(bucket)
                .maxKeys(1)
                .build();

        ListObjectsV2Iterable response = s3.listObjectsV2Paginator(listObjectsReqManual);

        for (S3Object companiesInfoFile : response.contents()) {

            System.out.println("Downloading companies XML: " + companiesInfoFile.key());

            // Prepare complete path
            Path targetPath = tagetDirectory.resolve(companiesInfoFile.key());
            if (Files.exists(targetPath)) {

                System.out.println("Object " + companiesInfoFile.key() + " has already been downloaded: ");
                continue;
            }

            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(companiesInfoFile.key())
                    .build();


            // Downloading object
            s3.getObject(objectRequest, ResponseTransformer.toFile(targetPath));

            // Retrieving basic metadata about object
            String key_string = companiesInfoFile.key();
            long size = companiesInfoFile.size();
            Instant lastModified = companiesInfoFile.lastModified();
            String eTag = companiesInfoFile.eTag();
            String storageClass = companiesInfoFile.storageClassAsString();

            System.out.println("Key: " + key_string);
            System.out.println("Size: " + size);
            System.out.println("Last Modified: " + lastModified);
            System.out.println("ETag: " + eTag);
            System.out.println("Storage Class: " + storageClass);

            System.out.println("Downloaded object: " + companiesInfoFile.key());
        }
    }
}
