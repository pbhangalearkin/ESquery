import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import java.util.Calendar;

import java.io.FileWriter;
import java.io.File;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class EsSearch {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        InetSocketTransportAddress inetAddress = new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300);
        TransportClient client = TransportClient.builder().settings(settings).build().addTransportAddress(inetAddress);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        Long start = cal.getTimeInMillis();
        QueryBuilder qb = QueryBuilders.rangeQuery("__start_ts").gt(start);
        SearchResponse scrollResp = client.prepareSearch()
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                //.addAggregation(AggregationBuilders.terms("key1"))
                .execute().actionGet();
        long totalHits = scrollResp.getHits().getTotalHits();
        long i = 0L;
        long totalDocumentSize = 0L;
        Multimap<String, String> objectTypeToObjectIdMap = ArrayListMultimap.create();
        class DocumentInfo {
            long documentCount;
            long totalDocumentSize;
        }
        Map<String, DocumentInfo> perObjectTypeDocumentSize = new HashMap<String, DocumentInfo>();
        while (scrollResp.getHits().getHits().length > 0) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String id = hit.getId();
                String[] parsedId = id.split(":", -1);
                long x = hit.sourceRef().length();

                if (!objectTypeToObjectIdMap.containsEntry(parsedId[1], parsedId[2]))
                    objectTypeToObjectIdMap.put(parsedId[1], parsedId[2]);
                if (!perObjectTypeDocumentSize.containsKey(parsedId[1])) {
                    DocumentInfo documentInfo = new DocumentInfo();
                    documentInfo.documentCount = 1;
                    documentInfo.totalDocumentSize = x;
                    perObjectTypeDocumentSize.put(parsedId[1], documentInfo);
                } else {
                    DocumentInfo documentInfo = perObjectTypeDocumentSize.get(parsedId[1]);
                    documentInfo.documentCount++;
                    documentInfo.totalDocumentSize += x;
                    perObjectTypeDocumentSize.put(parsedId[1], documentInfo);
                }
                totalDocumentSize += x;
                //System.out.println(x + " Bytes");
                //System.out.println(i + "/" + totalHits + ", Object Type : " + parsedId[1] + ", Object Id : " + parsedId[2]);
                i++;
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
//	System.out.println(objectTypeToObjectIdMap.values());
        System.out.println("Total Hits : " + totalHits);
        System.out.println("Average Document Size : " + (i > 0 ? totalDocumentSize / i : "NULL"));

        File file = new File("./output.csv");
        FileWriter csvWriter = new FileWriter(file);
        String delimiter = ",";
        String header = "ObjectType" + delimiter + "Number of Objects" + delimiter + "Average Document Size" + "\n";
        csvWriter.append(header);
        for (String key : objectTypeToObjectIdMap.keySet()) {
            String numOfObjects = Integer.toString(objectTypeToObjectIdMap.get(key).size());
            DocumentInfo documentInfo = perObjectTypeDocumentSize.get(key);
            String averageDocSize = Long.toString(documentInfo.totalDocumentSize / documentInfo.documentCount);
            String entry = key + delimiter + numOfObjects + delimiter + averageDocSize + "\n";
            csvWriter.append(entry);
        }
        csvWriter.flush();
        csvWriter.close();
    }
}