package com.sre.teaching.kafka.KafkaConsumerApp.datamodel;

import ch.qos.logback.classic.Logger;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.LoggerFactory;

public class CustomData {
    String id;
    String name;
    Float salary;
    Integer yearsOfExp;

    Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CustomData.class);

    public CustomData(String id, String name, Float salary, Integer yearsOfExp)
    {
        this.id=id;
        this.name=name;
        this.salary=salary;
        this.yearsOfExp=yearsOfExp;
    }

    public  CustomData()
    {

    }
    public void setId(String id)
    {
        this.id=id;
    }

    public  void setName(String name)
    {
        this.name=name;
    }

    public void setSalary(Float salary)
    {
        this.salary=salary;
    }

    public void  setYearsOfExp(Integer yearsOfExp)
    {
        this.yearsOfExp=yearsOfExp;
    }

    public String getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public Float getSalary()
    {
        return salary;
    }
    public  Integer getYearsOfExp()
    {
        return yearsOfExp;
    }

    public Boolean SetRecord(String idArg, String nameArg, String salaryArg, String experienceArg)
    {
        Boolean succeeded=true;

        try {
            id = idArg;
            name = nameArg;
            salary = Float.parseFloat(salaryArg);
            yearsOfExp = Integer.parseInt(experienceArg);
        }
        catch(NumberFormatException nex)
        {
            logger.error("Exception during setting the record data. Reason {}",nex.getMessage());
            succeeded=false;
        }
        return  succeeded;
    }

    @Override
    public String toString()
    {
        String jsonString = "CustomData{" +
                "ID="+id+
                ','+"Name='"+name+'\''+
                ','+"Salary="+salary+
                ','+"Experience="+yearsOfExp+
                '}';

        return  jsonString;
    }
}
