package com.alibaba.jstorm.tools.check;

import java.io.Serializable;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class HelloTuple implements Serializable {
	private static final long serialVersionUID = 1L;
	private String source;
	private String msgId;
	private Long emitTs;

	public HelloTuple() {
	}

	public HelloTuple(String source, String msgId, Long emitTs) {
		this.source = source;
		this.msgId = msgId;
		this.emitTs = emitTs;
	}

	public String getSource() {
		return this.source;
	}

	public String getMsgId() {
		return this.msgId;
	}

	public Long getEmitTs() {
		return this.emitTs;
	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
