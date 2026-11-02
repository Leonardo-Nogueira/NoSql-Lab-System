package com.leonardonogueira.nosqlmsg.mongodb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * DOCUMENT STORE (NoSQL - MongoDB):
 * Colecao: 'order_timelines'
 *
 * Repare no EMBEDDING (Documentos Embutidos):
 * - ShippingAddress e TimelineEvent moram DENTRO do mesmo documento BSON!
 * - Zero JOINs: 1 unica leitura traz todo o agregado de negocio!
 */
@Document(collection = "order_timelines")
public class OrderTimelineDocument {

    @Id
    private String id; // Gerado automaticamente como ObjectId hexadecimal do MongoDB

    @Indexed(unique = true) // Cria indice unico no Mongo para busca ultra-rapida por orderId!
    private String orderId;

    @Indexed // Indice para buscar todos os pedidos de um cliente
    private String customerId;

    private BigDecimal totalAmount;
    private String currentStatus;
    private ShippingAddress shippingAddress; // Documento Embutido (1:1)

    private List<TimelineEvent> timeline = new ArrayList<>(); // Array de Documentos Embutidos (1:N)
    private Instant createdAt;
    private Instant updatedAt;

    public OrderTimelineDocument() {}

    public OrderTimelineDocument(String orderId, String customerId, BigDecimal totalAmount, ShippingAddress shippingAddress) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.shippingAddress = shippingAddress;
        this.currentStatus = "CREATED";
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
        this.timeline.add(new TimelineEvent("CREATED", "Pedido registrado na plataforma", Instant.now()));
    }

    // Records aninhados representando os documentos embutidos (Embedding)
    public record ShippingAddress(String street, String number, String city, String state, String zipCode) {}
    public record TimelineEvent(String status, String description, Instant timestamp) {}

    public String getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public String getCurrentStatus() { return currentStatus; }
    public void setCurrentStatus(String currentStatus) { this.currentStatus = currentStatus; }
    public ShippingAddress getShippingAddress() { return shippingAddress; }
    public List<TimelineEvent> getTimeline() { return timeline; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}

