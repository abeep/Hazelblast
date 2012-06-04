package com.shop.api;


import java.io.Serializable;

import static com.hazelblast.utils.Arguments.notNull;

public class OrderLine implements Serializable {

    private final String articleId;
    private int quantity;

    public OrderLine(String articleId, int quantity) {
        this.articleId = notNull("articleId",articleId);
        this.quantity = quantity;
    }

    public String getArticleId() {
        return articleId;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public String toString() {
        return "OrderLine{" +
                "articleId='" + articleId + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
