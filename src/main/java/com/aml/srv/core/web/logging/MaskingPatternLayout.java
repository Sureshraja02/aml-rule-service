package com.aml.srv.core.web.logging;

import java.util.Optional;
import java.util.regex.Pattern;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class MaskingPatternLayout extends PatternLayout {

	private String maskPrefix;
	private Optional<Pattern> pattern;

	public String getMaskPrefix() {
		return maskPrefix;
	}

	public void setMaskPrefix(String maskPrefix) {
		this.maskPrefix = maskPrefix;
		if (this.maskPrefix != null) {
			this.pattern = Optional.of(Pattern.compile(maskPrefix, Pattern.MULTILINE));
		} else {
			this.pattern = Optional.empty();
		}
	}

	@Override
	public String doLayout(ILoggingEvent event) {
		String messageStr = (super.doLayout(event));
		return pattern.get().matcher(messageStr).replaceAll("*****");
	}
}
