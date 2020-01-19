package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Column;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.utils.EncryptUtil;


import java.util.Arrays;

/**
 * AES非对称加密
 * 注：只针对字符串类型
 * Created by tzq on 2020/1/19.
 */
public class AESTransformer extends Transformer {
    int columnIndex;
    public AESTransformer(){
        setTransformerName("dx_aes");
        System.out.println("Using AES preserve masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("dx_aes transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (oriValue == null) {
                return record;
            }
            if(column.getType() == Column.Type.STRING) {
                EncryptUtil encryptUtil = EncryptUtil.getInstance();
                String newValue = encryptUtil.AESencode(oriValue, "5uXApoCbL5VVwY4C");
                record.setColumn(columnIndex, new StringColumn(newValue));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
