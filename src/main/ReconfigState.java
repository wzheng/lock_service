package main;
import java.io.*;

public enum ReconfigState {
    CHANGE, // in preparation phase
	READY   // configuration not changing
};