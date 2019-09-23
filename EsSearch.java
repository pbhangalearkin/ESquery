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

import java.net.InetAddress;

public class EsSearch {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        InetSocketTransportAddress inetAddress = new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300);
        TransportClient client = TransportClient.builder().settings(settings).build().addTransportAddress(inetAddress);

        System.out.println("Here");
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
        System.out.println("Total Hits : "+totalHits);
        int i = 0;
        int totalDocumentSize = 0;
	while (scrollResp.getHits().getHits().length > 0) {
            BulkRequestBuilder builder = client.prepareBulk();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String id = hit.getId();
		int x = hit.sourceRef().length();
		totalDocumentSize += x;
		System.out.println(x + " Bytes"); 
//		System.out.println(hit.sourceAsString());
		System.out.println(i + "/" + totalHits + " " + id);
                i++;
            }
//            if (delete) {
//                builder.setRefresh(true);
//                BulkResponse response = builder.execute().get();
//                BulkItemResponse[] items = response.getItems();
//                for (BulkItemResponse item : items) {
//                    if (item.isFailed()) {
//                        System.out.println(item.getId() + " failed");
//                        System.out.println(item.getFailureMessage());
//                    } else {
//                        System.out.println(item.getId() + " success");
//                    }
//                }
//            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
	System.out.println("Average Document Size : " + ( i > 0 ? totalDocumentSize/i : "NULL")); 
    }
}
