# FastDuplicates
A duplicate video finder based in python

This is my first program im publishing.  So im relatively inexperienced, but was extremely happy with the results.

This is a basic program Ive created leveraging FFMPEG to extract frames at certain timestamps throughout the videos selected for comparison.  It utilizes a basic GUI and allows for selecting both files, and multiple folders.  Results are populated with information about each duplicate file and include a link to displaying the full file path that will take you directly to the file.

In my experience in its current version I can process around 900 videos in roughly 3 minutes this sample size was around 400gb though the size doesnt particularly matter as it is only looking for predetermined timestamps to extract frames and not analysing the full video.  Its approximately a 900% improvement over programs availble for purchase and has been around 98% accurate in its current iteration.  

As it is written it is not going to be usable for lower end PC's as it stores the frame captures in memory and processes in parellel with 8 workers (8 iterations of FFMPEG). Future iterations will intoduce batching with the ability for the program to detect system hardware and predetermine batch size.  This will obviously slow down analysis, but for the program to be more functional for more pcs and still achieve faster performance than current solutions it is the best method I know of with my limited experience.


Benchmark photo of performance:

"placeholder"
