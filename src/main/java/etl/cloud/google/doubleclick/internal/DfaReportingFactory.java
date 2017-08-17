// Based on class com.google.api.services.samples.dfareporting.DfaReportingFactory
// from https://github.com/googleads/googleads-dfa-reporting-samples

package etl.cloud.google.doubleclick.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.dfareporting.Dfareporting;
import com.google.api.services.dfareporting.DfareportingScopes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DfaReportingFactory {

    /**
     * Performs all necessary setup steps for running requests against the API.
     *
     * @param pathToClientSecretsFile Path to client secrets JSON file.
     * @return An initialized {@link Dfareporting} service object.
     */
    public static Dfareporting getInstance(String pathToClientSecretsFile) throws IOException {
        FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(USER_CREDS_STORE_DIR);

        Credential credential = authorize(pathToClientSecretsFile, dataStoreFactory);

        // Create Dfareporting client.
        return new Dfareporting.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    private static final String APPLICATION_NAME = "google-doubleclick-reporting";
    private static final File USER_CREDS_STORE_DIR = new File(System.getProperty("user.home"), APPLICATION_NAME);

    private static final HttpTransport HTTP_TRANSPORT = Utils.getDefaultTransport();
    private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();

    /**
    * Authorizes the installed application to access user's protected data.
    *
    * @param dataStoreFactory The data store to use for caching credential information.
    * @return A {@link Credential} object initialized with the current user's credentials.
    */
    private static Credential authorize(String pathToClientSecretsFile, DataStoreFactory dataStoreFactory) throws IOException {
        // Load client secrets JSON file.
        GoogleClientSecrets clientSecrets = GoogleClientSecrets
                .load(JSON_FACTORY, Files.newBufferedReader(Paths.get(pathToClientSecretsFile), UTF_8));

        // Set up the authorization code flow.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT,
            JSON_FACTORY, clientSecrets, DfareportingScopes.all()).setDataStoreFactory(dataStoreFactory)
            .build();

        // Authorize and persist credential information to the data store.
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    }

}
