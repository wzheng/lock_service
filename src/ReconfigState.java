import java.io.*;

public enum ReconfigState  {
	NONE,       // normal operation
	PREPARE,    // in preparation phase
	ACCEPT,     // 
	CHANGE      // cannot accept any more requests until reconfiguration is done
	};