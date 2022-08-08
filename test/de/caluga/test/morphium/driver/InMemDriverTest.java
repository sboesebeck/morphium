package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.Test;

public class InMemDriverTest {

    @Test
    public void inMemTest() throws Exception{
        InMemoryDriver drv=new InMemoryDriver();
        HelloCommand cmd=new HelloCommand(drv);
        cmd.executeAsync();
    }
}
