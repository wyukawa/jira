package azkaban.utils;


import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.sla.SlaOption;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;


public class Jira implements Alerter {

    private static Logger logger = Logger.getLogger(Jira.class);

    private final String azkabanName;

    private final String azkabanUrl;

    private final String jiraScheme;

    private final String jiraHost;

    private final String jiraPath;

    private final String jiraUser;

    private final String jiraPassword;

    private final String jiraProjectKey;

    private final String jiraIssuetypeId;

    private final String jiraIssueLabel;

    private final String jiraSummaryPrefix;

    public Jira(Props props) {
        this.azkabanName = props.getString("azkaban.name", "azkaban");
        this.azkabanUrl = props.getString("azkaban.url");
        this.jiraScheme = props.getString("jira.scheme");
        this.jiraHost = props.getString("jira.host");
        this.jiraPath = props.getString("jira.path");
        this.jiraUser = props.getString("jira.user");
        this.jiraPassword = props.getString("jira.password");
        this.jiraProjectKey = props.getString("jira.project.key");
        this.jiraIssuetypeId = props.getString("jira.issuetype.id");
        this.jiraIssueLabel = props.getString("jira.issue.label");
        this.jiraSummaryPrefix = props.getString("jira.summary.prefix");
    }

    @Override
    public void alertOnSuccess(ExecutableFlow exflow) throws Exception {
    }

    @Override
    public void alertOnError(ExecutableFlow exflow, String... extraReasons) throws Exception {
        sendJira(exflow, extraReasons);
    }

    private void sendJira(ExecutableFlow exflow, String... extraReasons) {

        try {
            String summary = jiraSummaryPrefix + " Flow '" + exflow.getFlowId() + "' has failed on " + azkabanName;

            int executionId = exflow.getExecutionId();
            String description = String.format("%s/executor?execid=%d", azkabanUrl, executionId);

            Map<String, Object> projectKey = new HashMap<String, Object>() {
                {
                    put("key", jiraProjectKey);
                }
            };
            Map<String, Object> issuetype = new HashMap<String, Object>() {
                {
                    put("id", jiraIssuetypeId);
                }
            };
            Map<String, Object> fields = new HashMap<String, Object>() {
                {
                    List<String> list = new ArrayList<>();
                    list.add(jiraIssueLabel);
                    put("project", projectKey);
                    put("summary", summary);
                    put("description", description);
                    put("issuetype", issuetype);
                    put("labels", list);
                }
            };

            Map<String, Object> map = new HashMap<>();
            map.put("fields", fields);

            String content = JSONUtils.toJSON(map);
            logger.info("request content=" + content);

            URIBuilder builder = new URIBuilder();
            builder.setScheme(jiraScheme).setHost(jiraHost).setPath(jiraPath);
            HttpPost httpPost = new HttpPost(builder.build());
            httpPost.setEntity(new StringEntity(content));

            List<Header> headerList = new ArrayList<Header>();
            String beforeBase64 = jiraUser + ":" + jiraPassword;
            String afterBase64 = Base64.getEncoder().encodeToString(beforeBase64.getBytes());
            headerList.add(new BasicHeader("Authorization", "Basic " + afterBase64));
            headerList.add(new BasicHeader("Content-Type", "application/json"));
            httpPost.setHeaders(headerList.toArray(new BasicHeader[0]));

            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpPost);
            final StatusLine statusLine = response.getStatusLine();
            String responseBody = response.getEntity() != null ?
                    EntityUtils.toString(response.getEntity()) : "";

            if (statusLine.getStatusCode() >= 300) {
                logger.error(String.format("unable to parse response as the response status is %s",
                        statusLine.getStatusCode()));

                throw new HttpResponseException(statusLine.getStatusCode(), responseBody);
            }

            logger.info("responseBody=" + responseBody);

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alertOnFirstError(ExecutableFlow exflow) throws Exception {
    }

    @Override
    public void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception {
    }

}
