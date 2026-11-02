package com.leonardonogueira.nosqlmsg.mongodb;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface OrderTimelineRepository extends MongoRepository<OrderTimelineDocument, String> {

    Optional<OrderTimelineDocument> findByOrderId(String orderId);

    List<OrderTimelineDocument> findByCustomerIdOrderByCreatedAtDesc(String customerId);

    List<OrderTimelineDocument> findByCurrentStatus(String currentStatus);
}

