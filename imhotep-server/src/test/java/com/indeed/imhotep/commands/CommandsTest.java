package com.indeed.imhotep.commands;

 interface CommandsTest {

     void testGetGroupStats() throws Exception;

     void testIntOrRegroup() throws Exception;

     void testTargetedMetricFilter() throws Exception;
    
     void testMetricRegroup() throws Exception; 
    
     void testMultiRegroup() throws Exception;
        
     void testMultiRegroupMessagesSender() throws Exception;
    
     void testMultiRegroupMessagesIterator() throws Exception; 

     void testUntargetedMetricFilter() throws Exception;
    
     void testRandomMetricMultiRegroup() throws Exception; 
    
     void testRandomMetricRegroup() throws Exception; 
    
     void testRandomMultiRegroup() throws Exception; 

     void testRandomRegroup() throws Exception; 
    
     void testRegexRegroup() throws Exception; 

     void testQueryRegroup() throws Exception;

     void testUnconditionalRegroup() throws Exception; 
    
     void testStringOrRegroup() throws Exception; 

}
