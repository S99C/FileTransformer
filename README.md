# FileTransformer
This python script reads an excel sheet and applies a set of transformations based on its type, by reading name of the file, if it contains "Enrollment" or "Usage" in the name.

**Steps to use it:**
1. Download the latest FileTransformer executable file from [here](https://github.com/S99C/FileTransformer/releases/tag/v1.0).
2. Put the executable file in the required folder. In the same directory as the executable, create another folder called 'FileTransform' (it will act as the input folder).
3. Put the 'Enrollment/Usage' files inside the folder that needs to modified and run the executable.
4. The output CSV file would would be saved inside the same 'FileTransform' folder.
5. When the executable is run, it should also create a 'Logs' folder in the same parent directory and generate new log files in it each time the executable is run.

Libraries Used:
Pandas,
OpenPyXL
