/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * _seq_no：严格递增的顺序号，每个文档一个，Shard级别严格递增，保证后写入的Doc的_seq_no大于先写入的Doc的_seq_no。
 *
 * 任何类型的写操作，包括index、create、update和Delete，都会生成一个_seq_no。这个是不可改的，而_version这个版本号是可以改的，后面会讲
 *
 * _primary_term：_primary_term也和_seq_no一样是一个整数，每当Primary Shard发生重新分配时，比如重启，Primary选举等，_primary_term会递增1。
 *
 * _primary_term主要是用来恢复数据时处理当多个文档的_seq_no一样时的冲突，比如当一个shard宕机了，raplica需要用到最新的数据，就会根据_primary_term和_seq_no这两个值来拿到最新的document
 */
public final class SeqNoPrimaryTermFetchSubPhase implements FetchSubPhase {
    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        if (context.seqNoAndPrimaryTerm() == false) {
            return;
        }

        hits = hits.clone(); // don't modify the incoming hits
        Arrays.sort(hits, Comparator.comparingInt(SearchHit::docId));

        int lastReaderId = -1;
        NumericDocValues seqNoField = null;
        NumericDocValues primaryTermField = null;
        for (SearchHit hit : hits) {
            int readerId = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
            LeafReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerId);
            if (lastReaderId != readerId) {
                seqNoField = subReaderContext.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
                primaryTermField = subReaderContext.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                lastReaderId = readerId;
            }
            int docId = hit.docId() - subReaderContext.docBase;
            long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
            // we have to check the primary term field as it is only assigned for non-nested documents
            if (primaryTermField != null && primaryTermField.advanceExact(docId)) {
                boolean found = seqNoField.advanceExact(docId);
                assert found: "found seq no for " + docId + " but not a primary term";
                seqNo = seqNoField.longValue();
                primaryTerm = primaryTermField.longValue();
            }
            hit.setSeqNo(seqNo);
            hit.setPrimaryTerm(primaryTerm);
        }
    }
}
