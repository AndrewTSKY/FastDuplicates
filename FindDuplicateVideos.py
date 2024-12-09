import sys
import os
import cv2
import numpy as np
import torch
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QPushButton, QLabel, QFileDialog, 
                            QProgressBar, QScrollArea, QFrame, QComboBox)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QImage, QPixmap
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import tempfile
import shutil
import imagehash
from PIL import Image
import random
import collections
import pathlib
import time

class VideoHasher(QThread):
    progress = pyqtSignal(int)
    groups_found = pyqtSignal(dict, dict, dict)
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)
    
    def __init__(self, video_paths):
        super().__init__()
        self.video_paths = video_paths
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    def extract_frames(self, video_path, timestamps):
        """Extract multiple frames at specified timestamps and store in memory"""
        frames = {}
        temp_dir = tempfile.mkdtemp()
        
        try:
            for ts in timestamps:
                frame_path = os.path.join(temp_dir, f'frame_{ts}.jpg')
                cmd = [
                    'ffmpeg', '-ss', str(ts),
                    '-i', video_path,
                    '-vframes', '1',
                    '-q:v', '2',
                    '-f', 'image2pipe',  # Output to pipe
                    '-pix_fmt', 'rgb24',  # Use RGB format
                    'pipe:'  # Output to stdout
                ]
                
                # Run ffmpeg and capture output directly
                result = subprocess.run(cmd, capture_output=True)
                
                if result.returncode == 0:
                    # Convert bytes to numpy array
                    frame_array = np.frombuffer(result.stdout, dtype=np.uint8)
                    if len(frame_array) > 0:
                        # Decode image from memory
                        frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                        if frame is not None:
                            # Convert to PIL Image for hashing
                            pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                            frames[ts] = {
                                'pil': pil_image,
                                'cv2': frame
                            }
                    
            return frames if frames else None
            
        except Exception as e:
            self.error_occurred.emit(f"Error extracting frames from {os.path.basename(video_path)}: {str(e)}")
            return None
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def compute_frame_hash(self, pil_image):
        """Compute perceptual hash of a frame"""
        # Resize to reasonable dimensions for hashing
        pil_image = pil_image.resize((64, 64), Image.Resampling.LANCZOS)
        return imagehash.average_hash(pil_image)

    def get_video_duration(self, video_path):
        """Get video duration using ffprobe"""
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            return float(result.stdout.strip())
        except:
            return 0

    def analyze_video(self, video_path):
        """Analyze video by sampling multiple frames"""
        self.status_update.emit(f"Analyzing: {os.path.basename(video_path)}")
        
        duration = self.get_video_duration(video_path)
        if duration < 10:  # Skip very short videos
            return None
            
        # Calculate sampling points
        # Skip first 30 seconds (to avoid title cards) and last 30 seconds (to avoid credits)
        start_time = min(30, duration * 0.1)  # 30 seconds or 10% of video
        end_time = max(0, duration - 30)  # 30 seconds from end
        
        if end_time <= start_time:
            return None
            
        # Sample 5 points from the main content of the video
        sample_duration = end_time - start_time
        timestamps = [
            start_time + (sample_duration * 0.2),  # 20% through main content
            start_time + (sample_duration * 0.4),  # 40% through main content
            start_time + (sample_duration * 0.6),  # 60% through main content
            start_time + (sample_duration * 0.8),  # 80% through main content
            start_time + (sample_duration * 0.9)   # 90% through main content
        ]
        
        frames = self.extract_frames(video_path, timestamps)
        if not frames or len(frames) < 3:  # Require at least 3 successful frame extractions
            return None
            
        # Compute hashes for each frame
        frame_hashes = {ts: self.compute_frame_hash(frames[ts]['pil']) for ts in frames}
        
        # Store the first frame for preview
        preview_frame = frames[timestamps[0]]['cv2']
        
        return {
            'duration': duration,
            'frame_hashes': frame_hashes,
            'preview': preview_frame
        }
            
        # Compute hashes for each frame
        frame_hashes = {ts: self.compute_frame_hash(frame) for ts, frame in frames.items()}
        
        # Save first frame as preview
        preview_frame = cv2.imread(os.path.join(tempfile.gettempdir(), f'frame_{timestamps[0]}.jpg'))
        
        return {
            'duration': duration,
            'frame_hashes': frame_hashes,
            'preview': preview_frame
        }

    def are_videos_similar(self, data1, data2):
        """Compare two videos based on multiple frame hashes"""
        # Duration check first
        duration_ratio = min(data1['duration'], data2['duration']) / max(data1['duration'], data2['duration'])
        if duration_ratio < 0.90:  # Allow 10% duration difference
            return False
        
        # Compare frame hashes
        matching_frames = 0
        total_comparisons = 0
        
        # Compare frames at similar timestamps
        for ts1, hash1 in data1['frame_hashes'].items():
            # Find closest timestamp in second video
            closest_ts2 = min(data2['frame_hashes'].keys(), key=lambda x: abs(x - ts1))
            hash2 = data2['frame_hashes'][closest_ts2]
            
            # Compare hashes with a small tolerance
            if hash1 - hash2 <= 4:  # Allow small differences
                matching_frames += 1
            total_comparisons += 1
        
        # Require at least 80% of frames to match
        return (matching_frames / total_comparisons) >= 0.8

    def find_duplicate_groups(self):
        """Find groups of duplicate videos using parallel video processing"""
        # Initialize data structures
        groups = []
        video_data = {}
        
        total_videos = len(self.video_paths)
        processed = 0
        
        # First pass: Analyze videos in parallel
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit all video analysis jobs
            future_to_path = {
                executor.submit(self.analyze_video, path): path 
                for path in self.video_paths
            }
            
            # Process results as they complete
            for future in as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    data = future.result()
                    if data is not None:
                        video_data[path] = data
                except Exception as e:
                    self.error_occurred.emit(f"Error analyzing {os.path.basename(path)}: {str(e)}")
                
                processed += 1
                self.progress.emit(int(processed * 50 / total_videos))
        
        # Second pass: Group similar videos with parallel comparison
        remaining_videos = set(video_data.keys())
        total_comparisons = len(remaining_videos)
        processed = 0
        
        while remaining_videos:
            video_path = remaining_videos.pop()
            current_group = {video_path}
            
            # Process comparisons in parallel
            compare_with = list(remaining_videos)
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_path = {
                    executor.submit(
                        self.are_videos_similar, 
                        video_data[video_path], 
                        video_data[other_path]
                    ): other_path
                    for other_path in compare_with
                }
                
                for future in as_completed(future_to_path):
                    other_path = future_to_path[future]
                    try:
                        if future.result():
                            current_group.add(other_path)
                            remaining_videos.remove(other_path)
                    except Exception as e:
                        self.error_occurred.emit(f"Error comparing videos: {str(e)}")
            
            if len(current_group) > 1:
                groups.append(current_group)
            
            processed += 1
            self.progress.emit(50 + int(processed * 50 / total_comparisons))
        
        # Create preview images
        group_previews = {}
        for group in groups:
            first_video = next(iter(group))
            preview_frame = video_data[first_video]['preview']
            if preview_frame is not None:
                preview_frame = cv2.cvtColor(preview_frame, cv2.COLOR_BGR2RGB)
                h, w, ch = preview_frame.shape
                q_img = QImage(preview_frame.data, w, h, ch * w, QImage.Format.Format_RGB888)
                group_previews[tuple(group)] = q_img
        
        self.status_update.emit("Analysis complete!")
        self.progress.emit(100)
        return groups, group_previews, video_data

    def run(self):
        """Main thread execution"""
        try:
            self.status_update.emit("Starting analysis...")
            groups, previews, video_data = self.find_duplicate_groups()
            self.groups_found.emit(dict(enumerate(groups)), previews, video_data)
        except Exception as e:
            self.error_occurred.emit(f"Error during processing: {str(e)}")

class ClickableLabel(QLabel):
    clicked = pyqtSignal()
    
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Video Duplicate Detector")
        self.setMinimumSize(800, 600)
        
        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # Selection mode and buttons
        top_layout = QHBoxLayout()

        # Add selection mode dropdown
        self.selection_mode = QComboBox()
        self.selection_mode.addItems(["Select Files", "Select Folders"])
        top_layout.addWidget(self.selection_mode)
        
        # Add buttons
        self.select_btn = QPushButton("Select")
        self.select_btn.clicked.connect(self.select_sources)
        self.start_btn = QPushButton("Start Scan")
        self.start_btn.clicked.connect(self.start_scan)
        self.start_btn.setEnabled(False)
        self.clear_btn = QPushButton("Clear Selection")
        self.clear_btn.clicked.connect(self.clear_selection)
        self.clear_btn.setEnabled(False)
        
        top_layout.addWidget(self.select_btn)
        top_layout.addWidget(self.clear_btn)
        top_layout.addWidget(self.start_btn)
        layout.addLayout(top_layout)

        # Selected sources display
        self.sources_label = QLabel("No files or folders selected")
        self.sources_label.setWordWrap(True)
        layout.addWidget(self.sources_label)
        
        # Status label
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet("color: blue;")
        layout.addWidget(self.status_label)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)
        
        # Error label
        self.error_label = QLabel()
        self.error_label.setStyleSheet("color: red;")
        layout.addWidget(self.error_label)
        
        # Scroll area for results
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.results_widget = QWidget()
        self.results_layout = QVBoxLayout(self.results_widget)
        scroll.setWidget(self.results_widget)
        layout.addWidget(scroll)
        
        self.video_paths = []
        self.selected_sources = []  # Store selected files and folders

    def format_size(self, size_bytes):
        """Format file size in human-readable format"""
        size_bytes = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"

    def format_duration(self, seconds):
        """Format duration in human-readable format"""
        seconds = float(seconds)
        if seconds < 60:
            return f"{seconds:.1f}s"
        minutes = int(seconds // 60)
        seconds = seconds % 60
        if minutes < 60:
            return f"{minutes}m {int(seconds)}s"
        hours = int(minutes // 60)
        minutes = minutes % 60
        return f"{hours}h {minutes}m"

    def get_video_files_from_folder(self, folder_path):
        """Recursively get all video files from a folder"""
        video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'}
        video_files = []
        
        try:
            # Count all video files first
            total_files = sum(1 for path in pathlib.Path(folder_path).rglob('*') 
                            if path.suffix.lower() in video_extensions)
            self.status_label.setText(f"Found {total_files} video files in folder...")
            
            # Then collect them
            for path in pathlib.Path(folder_path).rglob('*'):
                if path.suffix.lower() in video_extensions:
                    video_files.append(str(path))
        except Exception as e:
            self.error_label.setText(f"Error scanning folder {folder_path}: {str(e)}")
            
        return video_files
        
    def update_selected_sources_display(self):
        """Update the display of selected sources"""
        if not self.selected_sources:
            self.sources_label.setText("No files or folders selected")
            self.clear_btn.setEnabled(False)
            self.start_btn.setEnabled(False)
            return
            
        # Count files and folders
        files = [s for s in self.selected_sources if os.path.isfile(s)]
        folders = [s for s in self.selected_sources if os.path.isdir(s)]
        
        display_text = []
        if files:
            display_text.append(f"Selected {len(files)} files")
        if folders:
            display_text.append(f"Selected {len(folders)} folders")
            
        self.sources_label.setText(" | ".join(display_text))
        self.clear_btn.setEnabled(True)
        self.start_btn.setEnabled(True)
        
    def clear_selection(self):
        """Clear all selected sources"""
        self.selected_sources = []
        self.video_paths = []
        self.update_selected_sources_display()
        
    def select_sources(self):
        """Handle file or folder selection based on mode"""
        if self.selection_mode.currentText() == "Select Files":
            files, _ = QFileDialog.getOpenFileNames(
                self,
                "Select Videos",
                "",
                "Video Files (*.mp4 *.avi *.mkv *.mov *.wmv *.flv *.webm);;All Files (*)"
            )
            if files:
                self.selected_sources.extend(files)
                
        else:  # Select Folders
            folder = QFileDialog.getExistingDirectory(
                self,
                "Select Folder",
                "",
                QFileDialog.Option.ShowDirsOnly
            )
            if folder:
                self.selected_sources.append(folder)
                
        self.update_selected_sources_display()

    def open_file_location(self, path):
        """Open the folder containing the file and select it"""
        try:
            if sys.platform == 'win32':
                # Ensure the path is properly quoted
                path = path.replace('/', '\\')  # Convert forward slashes to backslashes
                os.system(f'explorer /select,"{path}"')
            elif sys.platform == 'darwin':  # macOS
                os.system(f'open -R "{path}"')
            else:  # Linux
                os.system(f'xdg-open "{os.path.dirname(path)}"')
        except Exception as e:
            self.error_label.setText(f"Error opening file location: {str(e)}")

    def display_groups(self, groups, previews, video_data):
        """Display the duplicate groups with previews and file information"""
        # Clear previous results
        for i in reversed(range(self.results_layout.count())): 
            self.results_layout.itemAt(i).widget().deleteLater()
            
        # Add a separator line
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        self.results_layout.addWidget(separator)
        
        # Display each group
        for group_id, files in groups.items():
            # Create group widget
            group_widget = QWidget()
            group_layout = QVBoxLayout(group_widget)
            
            # Add group header
            header = QLabel(f"Duplicate Group {group_id + 1} ({len(files)} files)")
            header.setStyleSheet("font-weight: bold; color: #2060A0;")
            group_layout.addWidget(header)
            
            # Add preview if available
            group_tuple = tuple(files)
            if group_tuple in previews:
                preview_label = QLabel()
                pixmap = QPixmap.fromImage(previews[group_tuple])
                pixmap = pixmap.scaled(200, 200, Qt.AspectRatioMode.KeepAspectRatio)
                preview_label.setPixmap(pixmap)
                group_layout.addWidget(preview_label)
            
            # Add file links with statistics
    def display_groups(self, groups, previews, video_data):
        """Display the duplicate groups with previews and file information"""
        # Clear previous results
        for i in reversed(range(self.results_layout.count())): 
            self.results_layout.itemAt(i).widget().deleteLater()
            
        # Add a separator line
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setFrameShadow(QFrame.Shadow.Sunken)
        self.results_layout.addWidget(separator)
        
        # Display each group
        for group_id, files in groups.items():
            # Create group widget
            group_widget = QWidget()
            group_layout = QVBoxLayout(group_widget)
            
            # Add group header
            header = QLabel(f"Duplicate Group {group_id + 1} ({len(files)} files)")
            header.setStyleSheet("font-weight: bold; color: #2060A0;")
            group_layout.addWidget(header)
            
            # Add preview if available
            group_tuple = tuple(files)
            if group_tuple in previews:
                preview_label = QLabel()
                pixmap = QPixmap.fromImage(previews[group_tuple])
                pixmap = pixmap.scaled(200, 200, Qt.AspectRatioMode.KeepAspectRatio)
                preview_label.setPixmap(pixmap)
                group_layout.addWidget(preview_label)
            
            # Add file links with statistics
            for file_path in files:
                file_widget = QWidget()
                file_layout = QVBoxLayout(file_widget)  # Changed to VBoxLayout for stacked display
                
                # Create clickable filename row
                filename_widget = QWidget()
                filename_layout = QHBoxLayout(filename_widget)
                
                # Clickable filename
                file_label = ClickableLabel(os.path.basename(file_path))
                file_label.setStyleSheet("color: blue; text-decoration: underline;")
                file_label.setCursor(Qt.CursorShape.PointingHandCursor)
                file_label.setToolTip("Click to open folder")
                file_label.clicked.connect(lambda p=file_path: self.open_file_location(p))
                filename_layout.addWidget(file_label)
                
                try:
                    file_size = os.path.getsize(file_path)
                    duration = video_data[file_path]['duration'] if file_path in video_data else 0
                    stats_label = QLabel(f"Size: {self.format_size(file_size)} | Duration: {self.format_duration(duration)}")
                    stats_label.setStyleSheet("color: #666666;")  # Gray color for stats
                    filename_layout.addWidget(stats_label)
                except Exception as e:
                    error_label = QLabel(f"Error getting file stats: {str(e)}")
                    error_label.setStyleSheet("color: red;")
                    filename_layout.addWidget(error_label)
                
                filename_layout.addStretch()
                file_layout.addWidget(filename_widget)
                
                # Add folder path below
                folder_path = os.path.dirname(file_path)
                if folder_path:
                    path_label = QLabel(f"Folder: {folder_path}")
                    path_label.setStyleSheet("color: #666666; font-size: 10pt;")  # Gray color for path
                    path_label.setWordWrap(True)
                    file_layout.addWidget(path_label)
                
                # Add file widget to group layout
                group_layout.addWidget(file_widget)
                
                # Add a thin separator line between files
                if file_path != list(files)[-1]:  # Don't add after last file
                    line = QFrame()
                    line.setFrameShape(QFrame.Shape.HLine)
                    line.setFrameShadow(QFrame.Shadow.Sunken)  # Fixed: Shadow is separate from Shape
                    line.setStyleSheet("background-color: #E0E0E0;")  # Light gray color
                    group_layout.addWidget(line)
            
            self.results_layout.addWidget(group_widget)
            
            # Add separator between groups
            separator = QFrame()
            separator.setFrameShape(QFrame.Shape.HLine)
            separator.setFrameShadow(QFrame.Shadow.Sunken)  # Fixed: Shadow is separate from Shape
            self.results_layout.addWidget(separator)

    def update_progress(self, value):
        """Update the progress bar value"""
        self.progress_bar.setValue(value)

    def show_error(self, error_message):
        """Display error message"""
        self.error_label.setText(error_message)

    def update_status(self, status):
        """Update status message"""
        self.status_label.setText(status)

    def start_scan(self):
        """Start the video duplicate detection process"""
        # Clear previous results
        for i in reversed(range(self.results_layout.count())): 
            self.results_layout.itemAt(i).widget().deleteLater()
        
        self.error_label.clear()
        self.select_btn.setEnabled(False)
        self.start_btn.setEnabled(False)
        self.clear_btn.setEnabled(False)
        
        # Start timer
        self.start_time = time.time()
        
        # Collect all video paths
        self.video_paths = []
        self.status_label.setText("Collecting video files...")
        
        # Process files and folders
        for source in self.selected_sources:
            if os.path.isfile(source):
                self.video_paths.append(source)
            else:  # It's a folder
                self.video_paths.extend(self.get_video_files_from_folder(source))
                
        if not self.video_paths:
            self.error_label.setText("No video files found in selected sources")
            self.select_btn.setEnabled(True)
            self.start_btn.setEnabled(True)
            self.clear_btn.setEnabled(True)
            return
            
        self.status_label.setText(f"Found {len(self.video_paths)} video files. Starting analysis...")
        
        # Start processing
        self.hasher = VideoHasher(self.video_paths)
        self.hasher.progress.connect(self.update_progress)
        self.hasher.groups_found.connect(self.display_groups)
        self.hasher.error_occurred.connect(self.show_error)
        self.hasher.status_update.connect(self.update_status)
        self.hasher.finished.connect(self.scan_finished)
        self.hasher.start()

    def scan_finished(self):
        """Re-enable buttons after scan is complete"""
        self.select_btn.setEnabled(True)
        self.start_btn.setEnabled(True)
        self.clear_btn.setEnabled(True)
        
        # Calculate elapsed time
        elapsed_time = time.time() - self.start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        # Update status with time taken
        self.status_label.setText(
            f"Analysis complete! Processed {len(self.video_paths)} files in {minutes}m {seconds}s"
        )

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
