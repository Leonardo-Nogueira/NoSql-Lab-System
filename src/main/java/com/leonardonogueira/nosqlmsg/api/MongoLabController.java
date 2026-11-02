package com.leonardonogueira.nosqlmsg.api;

import com.leonardonogueira.nosqlmsg.mongodb.OrderTimelineDocument;
import com.leonardonogueira.nosqlmsg.mongodb.OrderTimelineMongoService;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/mongodb")
public class MongoLabController {

    private final OrderTimelineMongoService service;

    public MongoLabController(OrderTimelineMongoService service) {
        this.service = service;
    }

    /**
     * TESTE 1: Criação de Documento BSON completo (Embedding de Endereço + Timeline)
     */
    @PostMapping("/orders")
    public OrderTimelineDocument createOrder(
            @RequestParam(defaultValue = "PED-2001") String orderId,
            @RequestParam(defaultValue = "CLI-LEO") String customerId,
            @RequestParam(defaultValue = "4999.00") BigDecimal amount,
            @RequestParam(defaultValue = "Avenida Paulista") String street,
            @RequestParam(defaultValue = "Sao Paulo") String city) {
        return service.createOrder(orderId, customerId, amount, street, city);
    }

    /**
     * TESTE 2: Atualização Atômica com $push e $set (sem ler o documento no Java)
     */
    @PostMapping("/orders/{orderId}/events")
    public Map<String, Object> appendEvent(
            @PathVariable String orderId,
            @RequestParam String status,
            @RequestParam String description) {
        return service.appendTimelineEventAtomically(orderId, status, description);
    }

    /**
     * TESTE 3: Leitura rápida do Documento por ID (Zero JOINs)
     */
    @GetMapping("/orders/{orderId}")
    public OrderTimelineDocument getOrder(@PathVariable String orderId) {
        return service.getByOrderId(orderId);
    }

    /**
     * TESTE 4: Consulta indexada por cliente
     */
    @GetMapping("/customers/{customerId}/orders")
    public List<OrderTimelineDocument> listByCustomer(@PathVariable String customerId) {
        return service.listByCustomer(customerId);
    }
}

