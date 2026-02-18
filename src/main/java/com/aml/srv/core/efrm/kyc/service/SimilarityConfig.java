package com.aml.srv.core.efrm.kyc.service;

import org.apache.commons.codec.language.Soundex;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SimilarityConfig {

    @Bean
    public JaroWinklerSimilarity jaroWinklerSimilarity() {
        return new JaroWinklerSimilarity();
    }
    
    
    @Bean
    public Soundex soundex()
    {
    	return new Soundex();
    }
}