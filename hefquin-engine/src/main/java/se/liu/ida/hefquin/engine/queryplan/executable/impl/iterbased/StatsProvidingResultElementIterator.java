package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

public interface StatsProvidingResultElementIterator extends ResultElementIterator
{
	int getNumberOfNexts();

	int getNumberOfThreadWakeUps();
}
