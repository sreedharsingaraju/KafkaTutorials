package com.example.kafkaproducer.datamodel;

import ch.qos.logback.classic.Logger;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.LoggerFactory;

public class CustomData {
    String id;
    String name;
    Float salary;
    Integer yearsOfExp;

    Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CustomData.class);

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
