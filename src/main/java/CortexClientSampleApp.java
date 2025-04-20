import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.dalet.mediator.cortex.ApiClient;
import com.dalet.mediator.cortex.ApiException;
import com.dalet.mediator.cortex.Configuration;
import com.dalet.mediator.cortex.api.AuthApi;
import com.dalet.mediator.cortex.api.JobsApi;
import com.dalet.mediator.cortex.auth.ApiKeyAuth;
import com.dalet.mediator.cortex.model.*;
import org.json.JSONObject;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class CortexClientSampleApp {

    /**
     * Mediator Client Id
     */
    private final String    client_id;

    /**
     * Mediator Client Secret
     */
    private final String    client_secret;

    /**
     * Project Service Id
     */
    private final String    project_service_id;


    /**
     * S3 client bucket name.
     */
    private final String    bucketName;
    private final String    inputKey;

    private final String    folderPath;
    private final String    inputFile;
	
    private final String    outputFile_json;
	private final String    outputFile_ttml;
	private final String    outputFile_text;
	
    private final String    outputKey_json;
    private final String    outputKey_ttml;
    private final String    outputKey_text;


	
    private final Integer   file_duration   = 30;

    private final String    basePath;
    private final String    region;
    private final String    language = "english";

    private final String    aws_access_key_id;
    private final String    aws_secret_access_key;
    private final String    aws_session_token;
	
		
    private ApiClient apiClient;
    private AmazonS3 s3Client;
    private JobsApi jobsApi;

    public static void main(String[] args) {
        try {
            CortexClientSampleApp impl = new CortexClientSampleApp(args);
            impl.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CortexClientSampleApp(String[] args) throws Exception{
        String data = new String(Files.readAllBytes(Paths.get(System.getenv("APP_CONFIG_FILE"))));
        if(data == null) {
            throw new ApiException("Configuration file 'app-config.json' is not found in " + System.getenv("APP_CONFIG_FILE"));
        }
        JSONObject config = new JSONObject(data);
        client_id = config.getString("clientKey");
        client_secret = config.getString("clientSecret");
        project_service_id = config.getString("projectServiceId");
        bucketName = config.getString("bucketName");
		region = config.has("bucketRegion") ? config.getString("bucketRegion") : "us-east-1";	
		
        inputKey = config.getString("inputFile");
        outputKey_json = config.getString("outputFile_json");
		outputKey_ttml = config.getString("outputFile_ttml");
		outputKey_text = config.getString("outputFile_text");		
        folderPath = config.getString("localPath");
 
		inputFile       =  folderPath + inputKey;
        outputFile_json = folderPath + outputKey_json;
		outputFile_ttml = folderPath + outputKey_ttml;
		outputFile_text = folderPath + outputKey_text;
		
        basePath = config.has("host") ? config.getString("host") : null;
		aws_access_key_id = config.has("aws_access_key_id") ? config.getString("aws_access_key_id") : null;
		aws_secret_access_key = config.has("aws_secret_access_key") ? config.getString("aws_secret_access_key") : null;
		aws_session_token = config.has("aws_session_token") ? config.getString("aws_session_token") : null;	
		
		if(aws_secret_access_key == null || aws_access_key_id == null) {
			throw new ApiException("AWS credentials are not defined in app-config.json file");
		}
    }

    private void run() throws ApiException, InterruptedException {
        /// credentials
        initAmazonS3Client();
        /// upload media ///////////
        uploadMediaToS3();
        // refresh auth token //////
        initializeDaletMediatorClient();
        // prepare job //////////////
        JobMediatorInput jobMediatorInput = prepareSpeechToTextMedatorJob();
        // post job ////////
        MediatorJob job = postJob(jobMediatorInput);
        // validate job ////
        validateJobResponse(job);
        // wait job to complete ////
        waitForComplete(job.getId());
        // download result ////////////
        downloadResult();
        // delete artifacts ////
        deleteS3artifacts();
    }

    private void deleteS3artifacts() {
        System.out.println("Deleting artifacts from S3 ...");
        s3Client.deleteObject(bucketName, inputKey);
		s3Client.deleteObject(bucketName, outputKey_json);
		s3Client.deleteObject(bucketName, outputKey_ttml);
		s3Client.deleteObject(bucketName, outputKey_text);
    }

    private void downloadResult() {
        System.out.println("Downloading results from S3 ...");
        File localFile_json = new File(outputFile_json);
        s3Client.getObject(new GetObjectRequest(bucketName, outputKey_json), localFile_json);
		
        File localFile_ttml = new File(outputFile_ttml);
        s3Client.getObject(new GetObjectRequest(bucketName, outputKey_ttml), localFile_ttml);

        File localFile_text = new File(outputFile_text);
        s3Client.getObject(new GetObjectRequest(bucketName, outputKey_text), localFile_text);		
    }

    private void waitForComplete(String jobId) throws ApiException {
        /// sleep between getjob requests - 30 seconds
        long sleepTime = 1000L * 30;
        do {
            System.out.println("Checking job status ...");
            MediatorJob mediatorJob = jobsApi.getJobById(jobId);
            // Validate job response for error
            validateJobResponse(jobsApi.getJobById(jobId));
            // check job for completed ///
            if (mediatorJob.getStatus().getStatus().equals(JobMediatorStatus.StatusEnum.COMPLETED)) {
                return;
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ApiException("Got interupted execption event. jobId: " + jobId);
            }
        }
        while (true);
    }

    protected void validateJobResponse(MediatorJob mediatorJob) throws ApiException {
        if (mediatorJob.getStatus().getStatus().equals(JobMediatorStatus.StatusEnum.FAILED)) {
            throw new ApiException("Cortex failed to perform the job. StatusMessage: [" + mediatorJob.getStatus().getStatusMessage() + "]");
        }
    }



    private MediatorJob postJob(JobMediatorInput jobMediatorInput) throws ApiException {
        System.out.println("Posting mediator job: " + jobMediatorInput.toString());
        return jobsApi.createJob(jobMediatorInput);
    }

    private void uploadMediaToS3() throws InterruptedException {
        System.out.println("Uploading media to S3 bucket ...");
        TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        Upload upload = tm.upload(bucketName, inputKey, new File(inputFile));
        System.out.println("---****************--->"+s3Client.getUrl(bucketName, inputKey).toString());
        upload.waitForCompletion();
        System.out.println("---*--->"+s3Client.getUrl(bucketName, inputKey).toString());
        System.out.println("--**->"+upload.getProgress());
        System.out.println("---***--->"+s3Client.getUrl(bucketName, inputKey).toString());
        System.out.println("--****->"+upload.getState());
        System.out.println("---*****--->"+s3Client.getUrl(bucketName, inputKey).toString());
        System.out.println("--******->0");
        System.out.println("Object upload complete");
        System.out.println("--**********-->1");
    }


    private void initAmazonS3Client() {
        System.out.println("Initializing amazon S3 client ...");
        // initialize credentials ///
        //ProfileCredentialsProvider provider = new ProfileCredentialsProvider();
        //awsCredentials = provider.getCredentials();
        // init amazon client
		AWSCredentials cr;
		if(aws_session_token == null) {
			cr = new BasicAWSCredentials(aws_access_key_id, aws_secret_access_key);
		} else {
			cr = new BasicSessionCredentials(aws_access_key_id, aws_secret_access_key, aws_session_token);
		}
        s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
				.withCredentials(new AWSStaticCredentialsProvider(cr))
                .withRegion(region != null ? Regions.fromName(region) : Regions.US_EAST_1)
                .build();
    }

    private JobMediatorInput prepareSpeechToTextMedatorJob() {
        // generate signed urls /////////////////
        System.out.println("Generating signed URLs ...");
        String inputSignedUrl = getS3PresignedUrl(bucketName, inputKey,  region);
		String outputSignedUrl_json = generatePutSignedUrl(bucketName, outputKey_json,  region);
		String outputSignedUrl_ttml = generatePutSignedUrl(bucketName, outputKey_ttml,  region);
		String outputSignedUrl_text = generatePutSignedUrl(bucketName, outputKey_text,  region);
		
	
		System.out.println("Generated input signed URL: " + inputSignedUrl);
		System.out.println("Generated output json signed URL: " + outputSignedUrl_json);
		System.out.println("Generated output ttml signed URL: " + outputSignedUrl_ttml);
		System.out.println("Generated output text signed URL: " + outputSignedUrl_text);

        // fill job data /////////////
        System.out.println("Preparing speech to text job data ...");
        Locator inputFile = new Locator()
                .awsS3Bucket(bucketName)
                .awsS3Key(inputKey)
                .httpEndpoint(inputSignedUrl);
				
				
        Locator jsonFormat = new Locator()
                .awsS3Bucket(bucketName)
                .awsS3Key(outputKey_json)
                .httpEndpoint(outputSignedUrl_json);
				
        Locator ttmlFormat = new Locator()
                .awsS3Bucket(bucketName)
                .awsS3Key(outputKey_ttml)
                .httpEndpoint(outputSignedUrl_ttml);
				
        Locator textFormat = new Locator()
                .awsS3Bucket(bucketName)
                .awsS3Key(outputKey_text)
                .httpEndpoint(outputSignedUrl_text);
				
								
		SpeechToTextOutput outputLocation = new SpeechToTextOutput().jsonFormat(jsonFormat).ttmlFormat(ttmlFormat).textFormat(textFormat);		
								
        JobInput spJobInput = new SpeechToTextInput()
                .inputFile(inputFile)
                .outputLocation(outputLocation)
                .language(language)
				.title("1234")
                .captionFormatStandard("CEA-608");

        Job aiJob = new Job()
                .jobType(Job.JobTypeEnum.AIJOB)
                .jobProfile(Job.JobProfileEnum.MEDIACORTEXSPEECHTOTEXT)
                .jobInput(spJobInput);

        JobMediatorInput jobMediatorInput = new JobMediatorInput()
                .projectServiceId(project_service_id)
                .quantity(file_duration)
				.tracking("tracking")
                .job(aiJob);

        return jobMediatorInput;
    }	
	
	
    private void initializeDaletMediatorClient() throws ApiException {
        apiClient = Configuration.getDefaultApiClient();
        // update mediator current path
        if(basePath != null) {
            apiClient.setBasePath(basePath);
        }
		System.out.println("Using mediator address: " + apiClient.getBasePath());

        System.out.println("Receiving mediator access token ...");
        AuthApi apiInstance = new AuthApi(apiClient);
        Token token = apiInstance.getAccessToken(client_id, client_secret);

        ApiKeyAuth tokenSignature = (ApiKeyAuth) apiClient.getAuthentication("tokenSignature");
        tokenSignature.setApiKey(token.getAuthorization());

        jobsApi = new JobsApi(apiClient);
    }

    private  String getS3PresignedUrl(String bucket, String key,  String region) {
        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60;// Set the presigned URL to expire after one hour.
        expiration.setTime(expTimeMillis);

/*
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withRegion(region != null ? Regions.fromName(region) : Regions.US_EAST_1)
                .build();
*/
		System.out.println("Generate signed URL for key: " + key);
        URL url = s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(bucket, key)
                .withMethod(HttpMethod.GET).withExpiration(expiration));
        return url.toString();
    }

    private  String generatePutSignedUrl(String bucketName, String key, String region) {
        /*
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                //.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsCredentials.getAWSAccessKeyId(), awsCredentials.getAWSSecretKey())))
                .withRegion(region != null ? Regions.fromName(region) : Regions.US_EAST_1)
                .build();
        */
        // Set the pre-signed URL to expire after four hour.
        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60 * 4;
        expiration.setTime(expTimeMillis);

        // Generate the pre-signed URL.
        System.out.println("Generating pre-signed URL.");
        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, key)
                .withMethod(HttpMethod.PUT)
                .withExpiration(expiration);
        return s3Client.generatePresignedUrl(generatePresignedUrlRequest).toString();
    }


}
