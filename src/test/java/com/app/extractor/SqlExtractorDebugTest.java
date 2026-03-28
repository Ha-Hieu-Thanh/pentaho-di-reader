package com.app.extractor;

import com.app.model.ExtractionResult;
import com.app.model.FileDetail;
import com.app.model.ColumnUsage;
import com.app.model.TableUsage;
import org.junit.Test;
import static org.junit.Assert.*;

public class SqlExtractorDebugTest {

    @Test
    public void debugSubscriberKtrFullRealQuery() throws Exception {
        SqlExtractor ex = new SqlExtractor();
        ExtractionResult result = new ExtractionResult();
        FileDetail fd = new FileDetail("test.ktr", "/tmp/test.ktr");

        // Exact first query from etl_subscriber.ktr (with vars as literals)
        String sql = "select " +
            "sub.product_code product_code, " +
            "aa.action_code, " +
            "aa.issue_datetime ngay_chuyen_doi, " +
            "aa.shop_code, " +
            "sub.status, " +
            "sub.act_status, " +
            "sub.create_user user_chuyen_doi, " +
            "sub.sub_id, " +
            "sub.isdn, " +
            "sub.reg_type_id, " +
            "d.code reason_code, " +
            "aa.action_audit_id, " +
            "aa.reason_id ly_do_doi_khuyenmai, " +
            "ad.old_value ma_km_truoc_doi, " +
            "ad.new_value ma_km_sau_doi, " +
            "sub.promotion_code ma_km_hientai, " +
            "sub.product_code ma_goi_cuoc_hientai, " +
            "spp.prepaid_code ma_cdt, " +
            "spp.prepaid_amount sotien_cdt, " +
            "sub.pay_type, " +
            "row_number() over (partition by sub.sub_id,aa.issue_datetime order by aa.issue_datetime DESC) rank " +
            "from f_action_audit aa " +
            "inner join subscriber sub on sub.sub_id = aa.pk_id and sub.product_code IN ('POBAS','D90U') and sub.status = 2 and sub.telecom_service_id = '1' " +
            "inner join f_sub_promotion_prepaid_pyc_1659 spp on spp.sub_id = sub.sub_id AND spp.status=1 AND FROM_UNIXTIME(UNIX_TIMESTAMP(spp.create_date),'yyyyMMddHHmmss')=aa.issue_datetime and spp.partition = '20240101' " +
            "left join f_action_detail ad on ad.action_audit_id=aa.action_audit_id and ad.issue_datetime=aa.issue_datetime and ad.col_name='PROMOTION_CODE' and ad.table_name='SUBSCRIBER' and ad.partition >= '20240101' and ad.partition < '20240201' " +
            "left join d_reason d on d.reason_id = sub.reg_type_id and d.status = 1 " +
            "where aa.partition >= '20240101' " +
            "and aa.partition < '20240201' " +
            "and aa.action_code in ('537') " +
            "and substr(aa.issue_datetime,1,8) >= '20240101' " +
            "and substr(aa.issue_datetime,1,8) < '20240201'";

        ex.extract(sql, fd, result);
        assertFalse("Should have tables", result.getTableUsagesSortedByCount().isEmpty());
        assertFalse("Should have columns", result.getColumnUsagesSortedByTableAndCount().isEmpty());

        // Verify key columns for each table
        assertTrue("f_action_audit should have action_code",
            result.getColumnUsagesSortedByTableAndCount().stream()
                .anyMatch(c -> c.getTableName().equals("f_action_audit") && c.getColumnName().equals("action_code")));
        assertTrue("subscriber should have sub_id",
            result.getColumnUsagesSortedByTableAndCount().stream()
                .anyMatch(c -> c.getTableName().equals("subscriber") && c.getColumnName().equals("sub_id")));
        assertTrue("d_reason should have code",
            result.getColumnUsagesSortedByTableAndCount().stream()
                .anyMatch(c -> c.getTableName().equals("d_reason") && c.getColumnName().equals("code")));
    }
}
