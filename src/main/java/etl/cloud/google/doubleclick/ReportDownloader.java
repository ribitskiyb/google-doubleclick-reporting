package etl.cloud.google.doubleclick;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.*;
import com.google.api.services.dfareporting.Dfareporting;
import com.google.api.services.dfareporting.model.DateRange;
import com.google.api.services.dfareporting.model.File;
import com.google.api.services.dfareporting.model.Report;
import etl.cloud.google.doubleclick.internal.DfaReportingFactory;
import etl.cloud.google.doubleclick.internal.ReportGenerationTimeoutException;

import java.io.*;


public class ReportDownloader {

    public ReportDownloader(String pathToClientSecretsFile) throws IOException {
        this.reporting = DfaReportingFactory.getInstance(pathToClientSecretsFile);
    }

    public void download(long profileId, long reportId, String startDate, String endDate,
                         String outputFile)
            throws IOException, InterruptedException, ReportGenerationTimeoutException {
        // Preparing a report patch for partial update
        DateRange dateRange = new DateRange()
                .setRelativeDateRange(Data.NULL_STRING)  // resetting relative_date_range attribute
                .setStartDate(new DateTime(startDate))
                .setEndDate(new DateTime(endDate));
        Report patch = new Report().setCriteria(new Report.Criteria().setDateRange(dateRange));

        // Update and get id of a new report
        long updatedReportId = reporting
                .reports()
                .patch(profileId, reportId, patch)
                .execute()
                .getId();

        File file = runReport(profileId, updatedReportId);
        downloadReport(reportId, file.getId(), outputFile);
    }

    private Dfareporting reporting;

    private File runReport(long profileId, long reportId)
            throws IOException, InterruptedException, ReportGenerationTimeoutException {
        long fileId = reporting.reports().run(profileId, reportId).execute().getId();

        BackOff backOff = new ExponentialBackOff.Builder()
                .setInitialIntervalMillis(10 * 1000)     // 10 second initial retry
                .setMaxIntervalMillis(10 * 60 * 1000)    // 10 minute maximum retry
                .setMaxElapsedTimeMillis(60 * 60 * 1000) // 1 hour total retry
                .build();

        while (true) {
            File file = reporting.files().get(reportId, fileId).execute();

            // Check to see if the report has finished processing
            if ("REPORT_AVAILABLE".equals(file.getStatus())) {
                return file;
            }

            // If the file isn't available yet, wait before checking again.
            long retryInterval = backOff.nextBackOffMillis();
            if (retryInterval == BackOff.STOP) {
                throw new ReportGenerationTimeoutException();
            }

            Thread.sleep(retryInterval);
        }
    }

    private void downloadReport(long reportId, long fileId, String outputFile) throws IOException {
        HttpResponse fileContents = reporting.files().get(reportId, fileId).executeMedia();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileContents.getContent(), Charsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
        } finally {
            fileContents.disconnect();
        }
    }

}
