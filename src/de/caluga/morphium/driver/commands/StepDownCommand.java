package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.Map;

public class StepDownCommand extends AdminMongoCommand<StepDownCommand> {
    private int timeToStepDown = 10;
    private Integer secondaryCatchUpPeriodSecs;
    private Boolean force;

    public StepDownCommand(MorphiumDriver d) {
        super(d);
    }

    @Override
    public String getCommandName() {
        return "replSetStepDown";
    }

    public int getTimeToStepDown() {
        return timeToStepDown;
    }

    public StepDownCommand setTimeToStepDown(int timeToStepDown) {
        this.timeToStepDown = timeToStepDown;
        return this;
    }

    public Integer getSecondaryCatchUpPeriodSecs() {
        return secondaryCatchUpPeriodSecs;
    }

    public StepDownCommand setSecondaryCatchUpPeriodSecs(Integer secondaryCatchUpPeriodSecs) {
        this.secondaryCatchUpPeriodSecs = secondaryCatchUpPeriodSecs;
        return this;
    }

    public Boolean getForce() {
        return force;
    }

    public StepDownCommand setForce(Boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public Map<String, Object> asMap() {
        var m = super.asMap();
        m.put(getCommandName(), timeToStepDown);
        m.remove("timeToStepDown");
        return m;
    }

}
