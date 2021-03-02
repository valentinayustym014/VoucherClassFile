package com.cas.backend.Model.VoucherType.VoucherType;

import com.cas.backend.Model.Exceptions.FilterNotAllowed;
import com.cas.backend.Model.Exceptions.VoucherTypeDoesNotExists;
import com.cas.backend.Model.Exceptions.VoucherTypeExistsException;
import com.cas.backend.Model.Filter.Filter;
import com.cas.backend.Model.Voucher.Voucher;
import com.cas.backend.Model.VoucherType.VoucherTypeEnum.VoucherParentTypeEnum;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


@Repository

public class VoucherTypeRepositoryImpl implements VoucherTypeRepositoryCustom {

    private final MongoTemplate mongoTemplate;


    @Autowired
    public VoucherTypeRepositoryImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List getVoucherTypeListForAutoComplete() {
        
        ProjectionOperation projectionOperation = Aggregation.project("id")
                .and("name").as("title")

                .and("voucherParent").as("subTitle");
        Aggregation aggregation = Aggregation.newAggregation(projectionOperation);
        AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, VoucherType.class, Document.class);
        return result.getMappedResults();
    }

    @Override
    public Page getListForDisplay(Pageable pageable, List<Filter> filters) throws FilterNotAllowed {

        SortOperation sortOperation = Aggregation.sort(Sort.Direction.ASC, "name");
        SkipOperation skipOperation = Aggregation.skip(pageable.getPageNumber() * pageable.getPageSize());
        LimitOperation limitOperation = Aggregation.limit(pageable.getPageSize());


        ProjectionOperation project = Aggregation.project("id")
                .and("name").as("Name")
                .and("voucherParent").as("Voucher_Parent")
                .and("printAfterSave").as("Print")
                .and("emailAfterSave").as("Email")
                .and("smsAfterSave").as("SMS");
        CountOperation countOperation = Aggregation.count().as("count");
        FacetOperation facetOperation = Aggregation.facet()
                .and(countOperation).as("count")
                .and( skipOperation, limitOperation).as("value");

        List<AggregationOperation> operations = new LinkedList<>();

        operations.add(sortOperation);

        operations.add(project);
        for(Filter filter: filters){
            operations.add(filter.getAggregationOperation());
        }
        operations.add(facetOperation);

        Aggregation aggregation = Aggregation.newAggregation(operations);


        AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, VoucherType.class, Document.class);
        List<Document> counts = result.getMappedResults().get(0).getList("count", Document.class);
        List<Document> values = result.getMappedResults().get(0).getList("value",Document.class);

        return new PageImpl<Document>(
                result.getMappedResults().get(0).getList("value",Document.class),
                pageable,
                counts.size()>0?counts.get(0).getInteger("count"): 0
        );

    }

  @Override
    public void saveVoucherType(VoucherType voucherType) throws VoucherTypeExistsException {
        if (uniqueVoucherType(voucherType.getName())){
            mongoTemplate.save(voucherType);
        }
        else{
            throw new VoucherTypeExistsException("The voucher type already exists. Please try a different voucher type name.");
        }
    }


    private boolean uniqueVoucherType(String name) {
        Query query = new Query();
        query.addCriteria(Criteria.where("name").is(name));
        List<VoucherType> voucherType = mongoTemplate.find(query, VoucherType.class);
        if(voucherType.size()>0){
            return false;
        }else{
            return true;
        }
    }

    @Override
    public void deleteVoucherType(ObjectId id) throws VoucherTypeDoesNotExists {

        Query removeQuery = new Query();
        removeQuery.addCriteria(Criteria.where("ObjectId._id").is(id));

        Query query = new Query();
        query.addCriteria(Criteria.where("voucherType").is(id));
        boolean voucherTypeExists  = mongoTemplate.exists(query, Voucher.class);

        if (!voucherTypeExists) {
            mongoTemplate.remove(removeQuery, VoucherType.class);
        }
        else {
            throw new VoucherTypeDoesNotExists(" This voucher type could not be deleted since it is currently being used.");
        }
    }

    @Override
    public List getVoucherTypes(String parent) {
        MatchOperation match = Aggregation.match(Criteria.where("voucherParent").is(parent));
        ProjectionOperation projectionOperation = Aggregation.project("id").and("name").as("name");
        Aggregation aggregation = Aggregation.newAggregation(match, projectionOperation);
        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, VoucherType.class, Document.class);
        return results.getMappedResults();
    }

    @Override
    public String updateNextVoucherNumber(ObjectId voucherTypeId) {
        Query search = new Query(Criteria.where("_id").is(voucherTypeId));
        Update update = new Update().inc("voucherNumberConfiguration.counter", 1);
        VoucherType counter = mongoTemplate.findAndModify(search, update, VoucherType.class);
        if (counter == null || counter.getVoucherNumberConfiguration()==null
                || counter.getVoucherNumberConfiguration().getCounter() == 0){
            VoucherNumberConfiguration conf = new VoucherNumberConfiguration();
            conf.setCounter(1);
            counter.setVoucherNumberConfiguration(conf);
            mongoTemplate.save(counter);
            return (conf.getPrefix()!= null?conf.getPrefix():"")
                    + conf.getCounter()
                    + (conf.getSuffix()!= null?conf.getSuffix():"");
        }

        return (counter.getVoucherNumberConfiguration().getPrefix()!= null?counter.getVoucherNumberConfiguration().getPrefix():"")
                + counter.getVoucherNumberConfiguration().getCounter()
                + (counter.getVoucherNumberConfiguration().getSuffix()!= null?counter.getVoucherNumberConfiguration().getSuffix():"");

    }

    @Override
    public void uploadAll(List<Document> stringsList) {
        List<VoucherType> groups = new LinkedList<>();
        Map<String, String> parentMap = new HashMap<>();
        for(Document document: stringsList){

            VoucherType voucherType = new VoucherType();
            try{

                    voucherType.setVoucherParent(VoucherParentTypeEnum.valueOfLabel(document.getString("voucherParentType")));
                    voucherType.setId(new ObjectId());
                    voucherType.setName(document.getString("name"));
                    groups.add(voucherType);


            }catch (NoSuchFieldException e){
                System.out.println("Voucher Parent " + document.getString("name")+" does not exists");
            }

        }

        mongoTemplate.insertAll(groups);


    }

    @Override
    public String getNextVoucherNumber(ObjectId voucherTypeId) {
        VoucherType counter = mongoTemplate.findById(voucherTypeId, VoucherType.class);
        if(counter.getVoucherNumberConfiguration() == null || counter.getVoucherNumberConfiguration().getCounter() == 0){
            return counter.getVoucherNumberConfiguration().getPrefix()
                    + 1
                    + counter.getVoucherNumberConfiguration().getSuffix();
        }
        return (counter.getVoucherNumberConfiguration().getPrefix()!= null?counter.getVoucherNumberConfiguration().getPrefix():"")
                + counter.getVoucherNumberConfiguration().getCounter()
                + (counter.getVoucherNumberConfiguration().getSuffix()!= null?counter.getVoucherNumberConfiguration().getSuffix():"");

    }


}
