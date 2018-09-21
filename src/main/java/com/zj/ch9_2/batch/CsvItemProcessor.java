package com.zj.ch9_2.batch;

import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;

import com.zj.ch9_2.domain.People;

public class CsvItemProcessor extends ValidatingItemProcessor<People> {

	@Override
	public People process(People item) throws ValidationException {
		super.process(item);//需执行super.process()才会调用自定义校验器
		if(item.getNation().equals("汉族")){
			item.setNation("01");
		}else{
			item.setNation("02");
		}
		return item;
	}
}
