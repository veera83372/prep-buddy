package com.thoughtworks.datacommons.prepbuddy.types

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord
import org.scalatest.FunSuite

class FileTypeTest extends FunSuite {
    test("should be able to parse CSV file and return a RowRecord") {
        val actual: RowRecord = CSV.parse("Name, Gender,Age")
    
        val expected: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))
    
        assert(actual(0) == expected(0))
        assert(actual(1) == expected(1))
        assert(actual(2) == expected(2))
    }
    
    test("should generate RowRecord into CSV format") {
        val record: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))
        
        val expected: String = "Name,Gender,Age"
        
        assert(expected == CSV.join(record))
    }
    
    test("should be able to parse TSV file and return a RowRecord") {
        val actual: RowRecord = TSV.parse("Name\t Gender\tAge")
        
        val expected: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))
        
        assert(actual(0) == expected(0))
        assert(actual(1) == expected(1))
        assert(actual(2) == expected(2))
    }
    
    test("should generate RowRecord into TSV format") {
        val record: RowRecord = new RowRecord(Array("Name", "Gender", "Age"))
        
        val expected: String = "Name\tGender\tAge"
        
        assert(expected == TSV.join(record))
    }
}
