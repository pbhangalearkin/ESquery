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

import java.net.InetAddress;

public class EsSearch {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        InetSocketTransportAddress inetAddress = new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300);
        TransportClient client = TransportClient.builder().settings(settings).build().addTransportAddress(inetAddress);

        String index = args[0];
        System.out.println("Here");
        QueryBuilder qb = QueryBuilders.termQuery("__cid", 10000);
        SearchResponse scrollResp = client.prepareSearch(index)
                .setScroll(new TimeValue(60000))
                //.addAggregation(AggregationBuilders.terms("key1"))
                .execute().actionGet();
        long totalHits = scrollResp.getHits().getTotalHits();
        System.out.println("Total Hits : "+totalHits);
        int i = 1;
        while (scrollResp.getHits().getHits().length > 0) {
            BulkRequestBuilder builder = client.prepareBulk();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String modelKey = (String) hit.getSource().get("modelKey");
                String id = hit.getId();
                System.out.println(i + "/" + totalHits + " " + id + " " + modelKey);
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
    }
}
