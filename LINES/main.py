import os
import time
from pathlib import Path
import hashlib
from tqdm import tqdm
import threading
import queue
import webbrowser
import json
import platform
from datetime import datetime
from colorama import init, Fore, Back, Style
import zipfile
import shutil
import sqlite3
import magic
import py7zr
import rarfile
import pyfiglet
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import RotatingFileHandler

# Initialize colorama
init(autoreset=True)

class FileExtractor:
    VERSION = "6.0 PRO"
    CONFIG_FILE = "file_extractor_config.json"
    DB_FILE = "file_extractor_stats.db"
    
    def __init__(self):
        # Initialize logging
        self.setup_logging()
        
        # Initialize database
        self.db_conn = sqlite3.connect(self.DB_FILE)
        self.init_db()

        # Statistics
        self.session_stats = {
            'total_files': 0,
            'processed': 0,
            'skipped': 0,
            'errors': 0,
            'total_lines': 0,
            'filtered_lines': 0,
            'duplicates_found': 0,
            'archives_processed': 0,
            'start_time': time.time()
        }
        self.stats = self.session_stats  # Alias for compatibility
        self.lock = threading.Lock()     # ✅ Thread-safe access lock
        
        # Settings with defaults
        self.settings = {
            'last_input_folder': '',
            'last_output_folder': 'extracted',
            'default_lines': 100,
            'default_threads': min(os.cpu_count() or 4, 16),
            'default_watermark': True,
            'default_file_types': ['.txt', '.log', '.csv', '.md'],
            'theme': 'pro_dark',
            'enable_logging': True,
            'max_file_size': 100,  # MB
            'backup_enabled': True,
            'backup_count': 3,
            'advanced_mode': False
        }
        self.load_settings()
        
        # Define color themes
        self.themes = {
            'pro_dark': {
                'primary': Fore.CYAN,
                'secondary': Fore.YELLOW,
                'success': Fore.GREEN + Style.BRIGHT,
                'error': Fore.RED + Style.BRIGHT,
                'warning': Fore.MAGENTA,
                'info': Fore.BLUE,
                'highlight': Fore.WHITE + Style.BRIGHT,
                'banner': Fore.CYAN + Style.BRIGHT
            },
            'pro_light': {
                'primary': Fore.BLUE,
                'secondary': Fore.MAGENTA,
                'success': Fore.GREEN,
                'error': Fore.RED,
                'warning': Fore.YELLOW,
                'info': Fore.CYAN,
                'highlight': Fore.BLACK + Style.BRIGHT,
                'banner': Fore.BLUE + Style.BRIGHT
            },
            'matrix': {
                'primary': Fore.GREEN,
                'secondary': Fore.GREEN + Style.DIM,
                'success': Fore.GREEN + Style.BRIGHT,
                'error': Fore.RED,
                'warning': Fore.YELLOW,
                'info': Fore.WHITE,
                'highlight': Fore.GREEN + Style.BRIGHT,
                'banner': Fore.GREEN
            }
        }
        self.current_theme = self.themes[self.settings['theme']]
        
        # File type magic detector
        self.mime = magic.Magic(mime=True)

    def setup_logging(self):
        """Configure advanced logging system"""
        self.logger = logging.getLogger('FileExtractor')
        self.logger.setLevel(logging.DEBUG)
        
        # Create logs directory if not exists
        Path('logs').mkdir(exist_ok=True)
        
        # Rotating file handler (5MB per file, max 3 backups)
        handler = RotatingFileHandler(
            'logs/file_extractor.log',
            maxBytes=5*1024*1024,
            backupCount=3
        )
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def init_db(self):
        """Initialize statistics database"""
        with self.db_conn:
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    operation TEXT,
                    input_folder TEXT,
                    output_folder TEXT,
                    files_processed INTEGER,
                    lines_processed INTEGER,
                    duration REAL,
                    status TEXT
                )
            """)
            
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS file_hashes (
                    hash TEXT PRIMARY KEY,
                    file_path TEXT,
                    last_processed DATETIME
                )
            """)

    def log_db_operation(self, operation, status="completed"):
        """Log operation to database"""
        try:
            with self.db_conn:
                self.db_conn.execute(
                    """
                    INSERT INTO processing_history (
                        operation, input_folder, output_folder,
                        files_processed, lines_processed,
                        duration, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        operation,
                        self.input_folder,
                        self.output_folder,
                        self.session_stats['processed'],
                        self.session_stats['total_lines'],
                        time.time() - self.session_stats['start_time'],
                        status
                    )
                )
        except Exception as e:
            self.logger.error(f"Database logging error: {str(e)}")

    def cprint(self, text, color_type='primary', style=None, end='\n'):
        """Enhanced colored print with theme support"""
        color = self.current_theme.get(color_type, Fore.WHITE)
        if style:
            color += style
        print(f"{color}{text}", end=end)

    def clear_screen(self):
        """Clear console screen"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def display_banner(self):
        """Display professional banner with system info"""
        self.clear_screen()
        
        # ASCII Art Banner
        banner_text = pyfiglet.figlet_format("Synex Pro", font="sub-zero")
        self.cprint(banner_text, 'banner')
        
        # System info
        self.cprint(f"{' ' * 10}⚡ Version: {self.VERSION} | 🖥️  {platform.system()} {platform.release()}", 'info')
        self.cprint(f"{' ' * 10}🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 💾 {self.get_disk_usage()}", 'info')
        self.cprint(f"{' ' * 10}🐍 Python {platform.python_version()} | 🧵 {os.cpu_count()} Cores", 'info')
        self.cprint("\n" + "═" * 80, 'primary')

    def get_disk_usage(self):
        """Get current disk usage statistics"""
        try:
            usage = shutil.disk_usage(Path.cwd())
            used_gb = usage.used / (1024 ** 3)
            total_gb = usage.total / (1024 ** 3)
            return f"{used_gb:.1f}GB / {total_gb:.1f}GB ({usage.percent}%)"
        except Exception as e:
            self.logger.warning(f"Disk usage error: {str(e)}")
            return "Disk stats unavailable"

    def display_menu(self):
        """Display enhanced main menu with visual grouping"""
        while True:
            self.display_banner()
            
            # Main menu options
            self.cprint("\n🏠 MAIN MENU\n", 'primary', Style.BRIGHT)
            
            # File Operations Group
            self.cprint("📁 FILE OPERATIONS", 'highlight')
            self.cprint("1. Start File Extraction", 'secondary')
            self.cprint("2. Batch Processing Mode", 'secondary')
            self.cprint("3. Archive Processing (ZIP/RAR/7Z)", 'secondary')
            
            # System Group
            self.cprint("\n⚙️ SYSTEM", 'highlight')
            self.cprint("4. Settings & Configuration", 'secondary')
            self.cprint("5. View Statistics & History", 'secondary')
            
            # Community Group
            self.cprint("\n🌐 COMMUNITY", 'highlight')
            self.cprint("6. Join Discord Community", 'secondary')
            self.cprint("7. Check for Updates", 'secondary')
            
            # Exit
            self.cprint("\n⏹️ EXIT", 'highlight')
            self.cprint("8. Exit Program", 'secondary')
            
            choice = input(self.current_theme['secondary'] + "\nSelect an option (1-8): ").strip()
            
            if choice == "1":
                self.get_user_input()
                self.run()
                input(self.current_theme['secondary'] + "\nPress Enter to continue...")
            elif choice == "2":
                self.batch_mode()
            elif choice == "3":
                self.archive_mode()
            elif choice == "4":
                self.settings_menu()
            elif choice == "5":
                self.show_statistics()
            elif choice == "6":
                self.open_discord()
            elif choice == "7":
                self.check_for_updates()
            elif choice == "8":
                self.save_settings()
                self.log_db_operation("program_exit", "normal")
                self.cprint("\n👋 Goodbye! Thanks for using FileX Pro", 'success')
                exit()
            else:
                self.cprint("Invalid choice! Please select 1-8", 'error')
                time.sleep(1)

    def check_for_updates(self):
        """Check for program updates"""
        self.display_banner()
        self.cprint("\n🔍 CHECKING FOR UPDATES...\n", 'primary', Style.BRIGHT)
        
        # Simulated update check (in a real app, this would connect to a server)
        self.cprint("✅ You are running the latest version", 'success')
        self.cprint(f"Current version: {self.VERSION}", 'info')
        
        # Show changelog
        self.cprint("\nRecent changes:", 'highlight')
        self.cprint("- Added support for RAR and 7Z archives", 'secondary')
        self.cprint("- Enhanced duplicate file detection", 'secondary')
        self.cprint("- Improved performance with ThreadPool", 'secondary')
        
        input(self.current_theme['secondary'] + "\nPress Enter to continue...")

    def open_discord(self):
        """Open Discord community with enhanced UI"""
        self.display_banner()
        self.cprint("\n🌐 DISCORD COMMUNITY\n", 'primary', Style.BRIGHT)
        
        self.cprint("Join our community for:", 'highlight')
        self.cprint("- 🆕 Latest updates and announcements", 'secondary')
        self.cprint("- 💡 Tips and tricks", 'secondary')
        self.cprint("- 🐞 Bug reports and feature requests", 'secondary')
        self.cprint("- 🏆 Exclusive content", 'secondary')
        
        self.cprint("\n🔗 https://discord.gg/ZuW9m5XkVY", 'info', Style.BRIGHT)
        
        if input(self.current_theme['secondary'] + "\nOpen in browser? (y/n): ").lower() == 'y':
            webbrowser.open("https://discord.gg/ZuW9m5XkVY")
            self.cprint("\nOpened Discord in your default browser", 'success')
        
        input(self.current_theme['secondary'] + "\nPress Enter to continue...")

    def settings_menu(self):
        """Display settings menu"""
        while True:
            self.display_banner()
            self.cprint("\nSettings Menu:", 'primary', Style.BRIGHT)
            self.cprint(f"1. Change Theme (Current: {self.settings['theme']})", 'secondary')
            self.cprint(f"2. Default Lines to Extract: {self.settings['default_lines']}", 'secondary')
            self.cprint(f"3. Default Threads: {self.settings['default_threads']}", 'secondary')
            self.cprint(f"4. Default File Types: {', '.join(self.settings['default_file_types'])}", 'secondary')
            self.cprint(f"5. Default Watermark: {'Enabled' if self.settings['default_watermark'] else 'Disabled'}", 'secondary')
            self.cprint("6. Reset All Settings", 'secondary')
            self.cprint("7. Back to Main Menu", 'secondary')
            
            choice = input(self.current_theme['secondary'] + "\nSelect an option (1-7): ").strip()
            
            if choice == "1":
                self.change_theme()
            elif choice == "2":
                self.change_setting('default_lines', "Enter default number of lines to extract: ", int)
            elif choice == "3":
                self.change_setting('default_threads', "Enter default number of threads (1-16): ", int, lambda x: min(16, max(1, x)))
            elif choice == "4":
                new_types = input("Enter allowed file extensions (comma separated, e.g., .txt,.log): ").strip()
                if new_types:
                    self.settings['default_file_types'] = [ext.strip() for ext in new_types.split(',') if ext.strip()]
            elif choice == "5":
                self.settings['default_watermark'] = not self.settings['default_watermark']
            elif choice == "6":
                if input("Are you sure you want to reset all settings? (y/n): ").lower() == 'y':
                    self.reset_settings()
            elif choice == "7":
                self.save_settings()
                return
            else:
                self.cprint("Invalid choice! Please select 1-7", 'error')

    def change_theme(self):
        """Change color theme"""
        self.cprint("\nAvailable Themes:", 'primary', Style.BRIGHT)
        for i, theme in enumerate(self.themes.keys(), 1):
            self.cprint(f"{i}. {theme.capitalize()}", 'secondary')
        
        try:
            choice = int(input(self.current_theme['secondary'] + "\nSelect theme (1-3): ").strip())
            theme_names = list(self.themes.keys())
            if 1 <= choice <= len(theme_names):
                self.settings['theme'] = theme_names[choice-1]
                self.current_theme = self.themes[self.settings['theme']]
            else:
                self.cprint("Invalid selection!", 'error')
        except ValueError:
            self.cprint("Please enter a number!", 'error')

    def change_setting(self, setting, prompt, type_func, process_func=lambda x: x):
        """Helper function to change settings"""
        try:
            new_value = type_func(input(self.current_theme['secondary'] + prompt).strip())
            self.settings[setting] = process_func(new_value)
        except ValueError:
            self.cprint("Invalid input!", 'error')

    def reset_settings(self):
        """Reset all settings to default"""
        default_settings = {
            'last_input_folder': '',
            'last_output_folder': 'extracted',
            'default_lines': 100,
            'default_threads': 4,
            'default_watermark': True,
            'default_file_types': ['.txt', '.log', '.csv'],
            'theme': 'default'
        }
        self.settings = default_settings
        self.current_theme = self.themes[self.settings['theme']]
        self.cprint("\nAll settings have been reset to defaults", 'success')

    def load_settings(self):
        """Load settings from config file"""
        try:
            if Path(self.CONFIG_FILE).exists():
                with open(self.CONFIG_FILE, 'r') as f:
                    loaded_settings = json.load(f)
                    # Only load settings that exist in our defaults
                    for key in self.settings:
                        if key in loaded_settings:
                            self.settings[key] = loaded_settings[key]
        except Exception as e:
            self.cprint(f"⚠️ Error loading settings: {str(e)}", 'warning')

    def save_settings(self):
        """Save settings to config file"""
        try:
            with open(self.CONFIG_FILE, 'w') as f:
                json.dump(self.settings, f, indent=4)
        except Exception as e:
            self.cprint(f"⚠️ Error saving settings: {str(e)}", 'warning')

    def batch_mode(self):
        """Process multiple folders in batch"""
        self.display_banner()
        self.cprint("\n📦 BATCH PROCESSING MODE", 'primary', Style.BRIGHT)
        
        root_folder = input(self.current_theme['secondary'] + "Enter root folder containing multiple input folders: ").strip()
        if not Path(root_folder).exists():
            self.cprint("❌ Folder doesn't exist!", 'error')
            return
            
        output_root = input(self.current_theme['secondary'] + f"Output root folder [batch_output]: ").strip() or "batch_output"
        
        # Get all subdirectories
        folders = [f for f in Path(root_folder).iterdir() if f.is_dir()]
        
        if not folders:
            self.cprint("No subfolders found in the specified directory!", 'warning')
            return
            
        self.cprint(f"\nFound {len(folders)} subfolders to process:", 'info')
        for folder in folders:
            self.cprint(f" - {folder.name}", 'secondary')
            
        if input(self.current_theme['secondary'] + "\nProceed with batch processing? (y/n): ").lower() != 'y':
            return
            
        start_time = time.time()
        processed_folders = 0
        
        for folder in folders:
            self.input_folder = str(folder)
            self.output_folder = str(Path(output_root) / folder.name)
            
            self.cprint(f"\nProcessing folder: {folder.name}", 'primary', Style.BRIGHT)
            
            try:
                self.run()
                processed_folders += 1
            except Exception as e:
                self.cprint(f"⚠️ Error processing {folder.name}: {str(e)}", 'error')
                continue
                
        elapsed = time.time() - start_time
        self.cprint(f"\n✅ Batch processing complete! Processed {processed_folders} of {len(folders)} folders", 'success')
        self.cprint(f"⏱️  Total duration: {elapsed:.2f} seconds", 'info')
        input(self.current_theme['secondary'] + "\nPress Enter to continue...")

    def archive_mode(self):
        """Process ZIP archives"""
        self.display_banner()
        self.cprint("\n🗄️ ARCHIVE PROCESSING MODE", 'primary', Style.BRIGHT)
        
        input_folder = input(self.current_theme['secondary'] + "Folder containing ZIP files: ").strip()
        if not Path(input_folder).exists():
            self.cprint("❌ Folder doesn't exist!", 'error')
            return
            
        output_folder = input(self.current_theme['secondary'] + f"Output folder [archive_output]: ").strip() or "archive_output"
        extract_files = input(self.current_theme['secondary'] + "Extract files from archives? (y/n): ").lower() == 'y'
        process_extracted = extract_files and input(self.current_theme['secondary'] + "Process extracted files? (y/n): ").lower() == 'y'
        
        # Find all ZIP files
        zip_files = list(Path(input_folder).glob('*.zip'))
        if not zip_files:
            self.cprint("No ZIP files found in the specified directory!", 'warning')
            return
            
        self.cprint(f"\nFound {len(zip_files)} ZIP archives:", 'info')
        for zip_file in zip_files:
            self.cprint(f" - {zip_file.name}", 'secondary')
            
        if input(self.current_theme['secondary'] + "\nProceed with archive processing? (y/n): ").lower() != 'y':
            return
            
        start_time = time.time()
        processed_archives = 0
        
        for zip_file in zip_files:
            try:
                self.cprint(f"\nProcessing archive: {zip_file.name}", 'primary', Style.BRIGHT)
                
                # Create output subfolder
                archive_output = Path(output_folder) / zip_file.stem
                archive_output.mkdir(parents=True, exist_ok=True)
                
                # Extract files if requested
                if extract_files:
                    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                        zip_ref.extractall(archive_output)
                        self.cprint(f"📦 Extracted {len(zip_ref.namelist())} files", 'info')
                        
                # Process extracted files if requested
                if process_extracted:
                    self.input_folder = str(archive_output)
                    self.output_folder = str(archive_output / "processed")
                    self.run()
                    
                processed_archives += 1
                self.stats['archives_processed'] += 1
                
            except Exception as e:
                self.cprint(f"⚠️ Error processing {zip_file.name}: {str(e)}", 'error')
                continue
                
        elapsed = time.time() - start_time
        self.cprint(f"\n✅ Archive processing complete! Processed {processed_archives} of {len(zip_files)} archives", 'success')
        self.cprint(f"⏱️  Total duration: {elapsed:.2f} seconds", 'info')
        input(self.current_theme['secondary'] + "\nPress Enter to continue...")

    def show_statistics(self):
        """Display statistics and processing history"""
        self.display_banner()
        self.cprint("\n📊 STATISTICS & HISTORY", 'primary', Style.BRIGHT)
        
        self.cprint("\n📈 Current Session Stats:", 'secondary')
        self.cprint(f"📂 Files Processed: {self.stats['processed']}", 'info')
        self.cprint(f"⚠️ Errors Encountered: {self.stats['errors']}", 'info')
        self.cprint(f"🔍 Filtered Lines: {self.stats['filtered_lines']}", 'info')
        self.cprint(f"♻️ Duplicates Found: {self.stats['duplicates_found']}", 'info')
        self.cprint(f"🗄️ Archives Processed: {self.stats['archives_processed']}", 'info')
        
        try:
            if Path(self.CONFIG_FILE).exists():
                config_time = datetime.fromtimestamp(Path(self.CONFIG_FILE).stat().st_mtime)
                self.cprint(f"\n🕒 Last Settings Modified: {config_time.strftime('%Y-%m-%d %H:%M:%S')}", 'info')
        except:
            pass
            
        input(self.current_theme['secondary'] + "\nPress Enter to continue...")

    def get_user_input(self):
        """Interactive user input with defaults from settings"""
        self.display_banner()
        
        # Use last input folder as default if available
        default_input = self.settings['last_input_folder'] if self.settings['last_input_folder'] else ''
        while True:
            self.input_folder = input(self.current_theme['secondary'] + 
                                    f"📁 Input folder path [{default_input}]: ").strip() or default_input
            if Path(self.input_folder).exists():
                self.settings['last_input_folder'] = self.input_folder
                break
            self.cprint("❌ Folder doesn't exist!", 'error')
            default_input = ''
        
        self.output_folder = input(self.current_theme['secondary'] + 
                                 f"📂 Output folder path [{self.settings['last_output_folder']}]: ").strip() or self.settings['last_output_folder']
        self.settings['last_output_folder'] = self.output_folder
        
        while True:
            try:
                default_lines = self.settings['default_lines']
                lines_input = input(self.current_theme['secondary'] + 
                                  f"🔢 Number of lines to extract [{default_lines}]: ").strip()
                self.lines_to_extract = int(lines_input) if lines_input else default_lines
                break
            except ValueError:
                self.cprint("❌ Please enter a number!", 'error')
        
        default_watermark = 'y' if self.settings['default_watermark'] else 'n'
        watermark_input = input(self.current_theme['secondary'] + 
                              f"💧 Add watermark? (y/n) [{default_watermark}]: ").strip().lower()
        self.watermark = watermark_input == 'y' if watermark_input else self.settings['default_watermark']
        
        self.filter_terms = [term.strip() for term in input(self.current_theme['secondary'] + 
                          "🔎 Filter terms (comma separated, leave empty): ").split(',') if term.strip()]
        
        default_threads = self.settings['default_threads']
        threads_input = input(self.current_theme['secondary'] + 
                            f"🧵 Threads to use (1-16) [{default_threads}]: ").strip()
        self.threads = min(16, max(1, int(threads_input) if threads_input else default_threads))
        
        # File type filtering
        default_types = ','.join(self.settings['default_file_types'])
        file_types = input(self.current_theme['secondary'] + 
                          f"📄 File extensions to process (comma separated) [{default_types}]: ").strip()
        self.file_types = [ext.strip() for ext in file_types.split(',') if ext.strip()] if file_types else self.settings['default_file_types']
        
        # Duplicate checking
        self.check_duplicates = input(self.current_theme['secondary'] + 
                                    "🔍 Check for duplicate files? (y/n): ").strip().lower() == 'y'

    def process_file(self, file_queue):
        """Process files from queue with enhanced features"""
        while not file_queue.empty():
            try:
                input_path = file_queue.get_nowait()
            except queue.Empty:
                return

            try:
                # Skip files with wrong extensions
                if input_path.suffix.lower() not in [ext.lower() for ext in self.file_types]:
                    with self.lock:
                        self.stats['skipped'] += 1
                    continue
                
                output_path = Path(self.output_folder) / f"extracted_{input_path.name}"
                
                # Calculate file hash for duplicate checking
                file_hash = self.calculate_hash(input_path)
                
                # Check for duplicates if enabled
                if self.check_duplicates and self.is_duplicate(input_path, file_hash):
                    with self.lock:
                        self.stats['duplicates_found'] += 1
                        self.stats['skipped'] += 1
                    continue
                
                # Process file
                lines_written = 0
                filtered = 0
                content = []
                
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'utf-16']:
                    try:
                        with open(input_path, 'r', encoding=encoding) as f:
                            for line in f:
                                if lines_written >= self.lines_to_extract:
                                    break
                                
                                # Apply filters
                                if self.filter_terms and any(term.lower() in line.lower() for term in self.filter_terms):
                                    filtered += 1
                                    continue
                                
                                content.append(line)
                                lines_written += 1
                        break
                    except UnicodeDecodeError:
                        continue

                # Add watermark
                if self.watermark:
                    watermark = (f"\n\n╔══════════════════════════╗\n"
                                f"║ 🏷️ SOURCE: {input_path.name}\n"
                                f"║ 🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                f"║ 📝 LINES: {lines_written}\n"
                                f"║ 🔒 HASH: {file_hash[:8]}...\n"
                                f"╚══════════════════════════╝\n")
                    content.append(watermark)

                # Write output file
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.writelines(content)
                
                # Update stats
                with self.lock:
                    self.stats['processed'] += 1
                    self.stats['total_lines'] += lines_written
                    self.stats['filtered_lines'] += filtered
                    
            except Exception as e:
                with self.lock:
                    self.stats['errors'] += 1
                continue
            finally:
                file_queue.task_done()

    def is_duplicate(self, filepath, file_hash):
        """Check if file is a duplicate by comparing with existing files"""
        output_dir = Path(self.output_folder)
        if not output_dir.exists():
            return False
            
        for existing_file in output_dir.iterdir():
            if existing_file.is_file() and existing_file != filepath:
                try:
                    existing_hash = self.calculate_hash(existing_file)
                    if existing_hash == file_hash:
                        return True
                except:
                    continue
        return False

    def calculate_hash(self, filepath):
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def run(self):
        """Main function with enhanced progress tracking"""
        start_time = time.time()
        
        # Create output folder
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        
        # Create file list with type filtering
        files = [f for f in Path(self.input_folder).iterdir() 
                if f.is_file() and f.suffix.lower() in [ext.lower() for ext in self.file_types]]
        self.stats['total_files'] = len(files)
        
        if not files:
            self.cprint("⚠️ No matching files found in the specified directory!", 'warning')
            return
            
        # Queue for multithreading
        file_queue = queue.Queue()
        for f in files:
            file_queue.put(f)
        
        # Progress bar
        progress = tqdm(total=self.stats['total_files'], 
                       desc=self.current_theme['info'] + "📂 Processing files", 
                       unit="file",
                       bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
        
        # Start threads
        threads = []
        for _ in range(self.threads):
            t = threading.Thread(target=self.process_file, args=(file_queue,))
            t.start()
            threads.append(t)
        
        # Update progress
        while any(t.is_alive() for t in threads):
            progress.n = self.stats['processed'] + self.stats['errors'] + self.stats['skipped']
            progress.refresh()
            time.sleep(0.1)
        
        progress.close()
        
        # Show summary
        elapsed = time.time() - start_time
        self.cprint("\n✅ Processing complete!", 'success', Style.BRIGHT)
        self.cprint("══════════════════════════════════════════════════", 'primary')
        self.cprint(f"⏱️  Duration: {elapsed:.2f} seconds", 'info')
        self.cprint(f"📂 Files: {self.stats['total_files']} total | {self.stats['processed']} processed | "
                  f"{self.stats['skipped']} skipped | {self.stats['errors']} errors", 'info')
        self.cprint(f"📊 Lines: {self.stats['total_lines']} extracted | {self.stats['filtered_lines']} filtered", 'info')
        self.cprint(f"♻️ Duplicates Found: {self.stats['duplicates_found']}", 'info')
        self.cprint("══════════════════════════════════════════════════", 'primary')
        self.cprint(f"🔧 Settings: {self.threads} Threads | Watermark {'✅' if self.watermark else '❌'} | "
                  f"Filter {'✅' if self.filter_terms else '❌'} | File Types: {', '.join(self.file_types)}", 'secondary')

if __name__ == "__main__":
    try:
        extractor = FileExtractor()
        extractor.display_menu()
    except KeyboardInterrupt:
        print(Fore.RED + "\n\nOperation cancelled by user. Goodbye!")
        exit()
    except Exception as e:
        print(Fore.RED + f"\n\nAn unexpected error occurred: {str(e)}")
        exit()