package com.bin.es.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @ClassName: SearchEngine
 * @Author: bin.yang
 * @Date: 2019/4/9 12:04
 * @Description: TODO
 */
@Data
public class SearchEngine implements Serializable {

    private static final long serialVersionUID = 7112016393604834708L;
    
    private Integer page = 1;

    private Integer size = 10;

    private String keyword = null;

    private Integer start;

    public Integer start() {

        return (page -1 ) * size;
    }

    public Integer end() {
        return size;
    }

    public String keyword() {
        return keyword;
    }
}
