import connectwithinfinitydb as db
import tkinter as tk
from tkinter import messagebox
import time
import threading
from datetime import datetime, timedelta
import ctypes
import sys

# Windows API constants for idle time detection
USER_TIMER_MAXIMUM = 0x7FFFFFFF

def get_idle_duration():
    """Get system idle time in minutes"""
    try:
        # For Windows
        if sys.platform == 'win32':
            class LASTINPUTINFO(ctypes.Structure):
                _fields_ = [
                    ('cbSize', ctypes.c_uint),
                    ('dwTime', ctypes.c_uint)
                ]
            
            lastInputInfo = LASTINPUTINFO()
            lastInputInfo.cbSize = ctypes.sizeof(LASTINPUTINFO)
            
            if ctypes.windll.user32.GetLastInputInfo(ctypes.byref(lastInputInfo)):
                millis = ctypes.windll.kernel32.GetTickCount() - lastInputInfo.dwTime
                return millis / 1000.0 / 60.0  # Convert to minutes
        return 0
    except:
        return 0

def parse_inactivity_setting(setting):
    """Parse inactivity setting like '1 minute', '2 hours', '3 days', '1 month'"""
    try:
        parts = setting.strip().lower().split()
        if len(parts) != 2:
            return None, None
        
        value = int(parts[0])
        unit = parts[1]
        
        if unit in ['minute', 'minutes']:
            return 'minutes', value
        elif unit in ['hour', 'hours']:
            return 'hours', value
        elif unit in ['day', 'days']:
            return 'days', value
        elif unit in ['month', 'months']:
            return 'months', value
        else:
            return None, None
    except:
        return None, None

def convert_to_minutes(unit, value):
    """Convert different time units to minutes"""
    if unit == 'minutes':
        return value
    elif unit == 'hours':
        return value * 60
    elif unit == 'days':
        return value * 24 * 60
    elif unit == 'months':
        return value * 30 * 24 * 60  # Approximate month as 30 days
    return 0

class ServerAuthWindow:
    def __init__(self, inactivity_config="1 hour"):
        """
        Initialize the server authentication window with lockout mechanism
        
        Args:
            inactivity_config: String like "1 minute", "2 hours", "3 days", "1 month"
        """
        self.inactivity_config = inactivity_config
        self.failed_attempts = 0
        self.max_attempts = 6
        self.lockout_time = 0
        self.lockout_until = None
        self.is_locked = False
        self.auth_success = False
        self.server_id = None
        self.window_destroyed = True
        
        # Parse inactivity setting
        self.inactivity_unit, self.inactivity_value = parse_inactivity_setting(inactivity_config)
        if self.inactivity_unit is None:
            print(f"Invalid inactivity config: {inactivity_config}. Using default: 1 hour")
            self.inactivity_unit = 'hours'
            self.inactivity_value = 1
        
        self.inactivity_minutes = convert_to_minutes(self.inactivity_unit, self.inactivity_value)
        
        # Main window variables
        self.overlay_root = None
        self.password_entry = None
        self.status_label = None
        self.lockout_label = None
        self.confirm_btn = None
        self.cancel_btn = None
        self.dialog_frame = None
        self.main_thread_id = threading.current_thread()
        
        # Start inactivity monitor thread
        self.monitoring = True
        self.inactivity_thread = threading.Thread(target=self.monitor_inactivity, daemon=True)
        self.inactivity_thread.start()
        
        # Flag for after callbacks
        self.after_id = None
        
    def calculate_lockout_duration(self):
        """Calculate lockout duration based on failed attempts"""
        if self.failed_attempts <= 0:
            return 0
        return 30 * self.failed_attempts
    
    def update_lockout_display(self):
        """Update the lockout countdown display"""
        if self.is_locked and self.lockout_until:
            remaining = max(0, (self.lockout_until - datetime.now()).total_seconds())
            
            if remaining <= 0:
                self.is_locked = False
                self.lockout_until = None
                self.enable_input()
                return
            
            minutes = int(remaining // 60)
            seconds = int(remaining % 60)
            
            if self.lockout_label and self.lockout_label.winfo_exists():
                self.lockout_label.config(
                    text=f"⏰ LOCKED: Please wait {minutes}m {seconds}s before trying again",
                    fg='#e74c3c'
                )
            
            if self.password_entry and self.password_entry.winfo_exists():
                self.password_entry.config(state='disabled')
            if self.confirm_btn and self.confirm_btn.winfo_exists():
                self.confirm_btn.config(state='disabled')
            if self.cancel_btn and self.cancel_btn.winfo_exists():
                self.cancel_btn.config(state='disabled')
            
            # Schedule next update
            if self.overlay_root and self.overlay_root.winfo_exists():
                self.after_id = self.overlay_root.after(1000, self.update_lockout_display)
        else:
            if self.lockout_label and self.lockout_label.winfo_exists():
                self.lockout_label.config(text="")
    
    def enable_input(self):
        """Enable input fields after lockout"""
        try:
            if self.password_entry and self.password_entry.winfo_exists():
                self.password_entry.config(state='normal')
                self.password_entry.delete(0, tk.END)
                self.password_entry.focus_set()
            if self.confirm_btn and self.confirm_btn.winfo_exists():
                self.confirm_btn.config(state='normal')
            if self.cancel_btn and self.cancel_btn.winfo_exists():
                self.cancel_btn.config(state='normal')
            if self.lockout_label and self.lockout_label.winfo_exists():
                self.lockout_label.config(text="")
            if self.status_label and self.status_label.winfo_exists():
                self.status_label.config(text="Enter server password", fg='#95a5a6')
        except:
            pass
    
    def handle_failed_attempt(self):
        """Handle failed authentication attempt"""
        self.failed_attempts += 1
        
        if self.failed_attempts >= self.max_attempts:
            self.permanent_lock()
            return
        
        lockout_seconds = self.calculate_lockout_duration()
        self.lockout_until = datetime.now() + timedelta(seconds=lockout_seconds)
        self.is_locked = True
        
        remaining_attempts = self.max_attempts - self.failed_attempts
        messagebox.showwarning(
            "Authentication Failed",
            f"✗ Invalid password or cancelled!\n\n"
            f"Attempt {self.failed_attempts} of {self.max_attempts}\n"
            f"Please wait {lockout_seconds} seconds before trying again.\n"
            f"Remaining attempts: {remaining_attempts}"
        )
        
        self.update_lockout_display()
    
    def permanent_lock(self):
        """Permanent lock when max attempts reached"""
        self.is_locked = True
        self.auth_success = False
        
        try:
            if self.dialog_frame and self.dialog_frame.winfo_exists():
                for widget in self.dialog_frame.winfo_children():
                    widget.destroy()
            
                lock_frame = tk.Frame(self.dialog_frame, bg='#2c3e50')
                lock_frame.pack(fill='both', expand=True, padx=20, pady=20)
                
                lock_icon = tk.Label(
                    lock_frame,
                    text="🔒",
                    font=('Arial', 48),
                    bg='#2c3e50',
                    fg='#e74c3c'
                )
                lock_icon.pack(pady=(20, 10))
                
                lock_title = tk.Label(
                    lock_frame,
                    text="ACCESS DENIED",
                    font=('Arial', 16, 'bold'),
                    bg='#2c3e50',
                    fg='#e74c3c'
                )
                lock_title.pack(pady=(0, 10))
                
                lock_message = tk.Label(
                    lock_frame,
                    text=f"Maximum attempts ({self.max_attempts}) exceeded.\n\n"
                         f"Please contact the server administrator.\n\n"
                         f"This window cannot be closed.\n"
                         f"Restart the application to try again.",
                    font=('Arial', 11),
                    bg='#2c3e50',
                    fg='#ecf0f1',
                    justify='center'
                )
                lock_message.pack(pady=(10, 20))
                
                if self.password_entry and self.password_entry.winfo_exists():
                    self.password_entry.config(state='disabled')
                
                for btn in [self.confirm_btn, self.cancel_btn]:
                    if btn and btn.winfo_exists():
                        btn.config(state='disabled')
                
                if self.status_label and self.status_label.winfo_exists():
                    self.status_label.config(text="CONTACT ADMINISTRATOR", fg='#e74c3c')
        except:
            pass
    
    def verify_password(self, password):
        """Verify password against database"""
        try:
            query = f"SELECT server_id, server_passkey FROM server_auth WHERE server_passkey = '{password}'"
            result = db.execute_query(query)
            
            if result.get('status') != 'success':
                return {'status': 'error', 'message': f"Database query failed: {result.get('message', 'Unknown error')}", 'server_id': None}
            
            results = result.get('results', [])
            
            if results and len(results) > 0:
                server_id = results[0].get('server_id', 'Unknown')
                return {'status': 'success', 'message': "Authentication successful", 'server_id': server_id}
            else:
                return {'status': 'error', 'message': "Invalid server password", 'server_id': None}
                
        except Exception as e:
            return {'status': 'error', 'message': f"Authentication error: {str(e)}", 'server_id': None}
    
    def show_success_message(self, server_id):
        """Show success message in a small window"""
        temp_root = tk.Tk()
        temp_root.title("Success")
        temp_root.geometry("400x200")
        temp_root.attributes('-topmost', True)
        
        temp_root.update_idletasks()
        width = 400
        height = 200
        x = (temp_root.winfo_screenwidth() // 2) - (width // 2)
        y = (temp_root.winfo_screenheight() // 2) - (height // 2)
        temp_root.geometry(f'{width}x{height}+{x}+{y}')
        
        frame = tk.Frame(temp_root, bg='#2c3e50', padx=20, pady=20)
        frame.pack(fill='both', expand=True)
        
        success_icon = tk.Label(frame, text="✓", font=('Arial', 48), bg='#2c3e50', fg='#27ae60')
        success_icon.pack(pady=(10, 5))
        
        msg_label = tk.Label(
            frame,
            text=f"Authentication Successful!\n\nServer ID: {server_id}\n\nAccess Granted.",
            font=('Arial', 11),
            bg='#2c3e50',
            fg='#ecf0f1',
            justify='center'
        )
        msg_label.pack(pady=(10, 20))
        
        ok_btn = tk.Button(
            frame,
            text="OK",
            font=('Arial', 10, 'bold'),
            bg='#27ae60',
            fg='white',
            activebackground='#229954',
            relief='flat',
            padx=20,
            pady=5,
            command=temp_root.destroy
        )
        ok_btn.pack()
        
        temp_root.after(3000, lambda: temp_root.destroy() if temp_root.winfo_exists() else None)
        temp_root.mainloop()
    
    def on_confirm(self):
        """Handle confirm button click"""
        if self.is_locked:
            messagebox.showwarning("Locked", f"Please wait for lockout period to end.")
            return
        
        if not self.password_entry or not self.password_entry.winfo_exists():
            return
            
        entered_password = self.password_entry.get()
        
        if not entered_password:
            messagebox.showwarning("Input Required", "Please enter the server password.")
            self.password_entry.focus_set()
            return
        
        if self.confirm_btn and self.confirm_btn.winfo_exists():
            self.confirm_btn.config(state='disabled', text="Verifying...")
            self.overlay_root.update()
        
        try:
            verification = self.verify_password(entered_password)
            
            if verification['status'] == 'success':
                self.server_id = verification.get('server_id')
                
                # Destroy the overlay window
                if self.overlay_root and self.overlay_root.winfo_exists():
                    # Cancel any pending after callbacks
                    if self.after_id:
                        try:
                            self.overlay_root.after_cancel(self.after_id)
                        except:
                            pass
                    
                    overlay_root_temp = self.overlay_root
                    self.overlay_root = None
                    overlay_root_temp.quit()
                    overlay_root_temp.destroy()
                    self.window_destroyed = True
                
                self.auth_success = True
                self.failed_attempts = 0
                self.is_locked = False
                self.lockout_until = None
                
                # Show success message in a separate window
                self.show_success_message(self.server_id)
                
                return
                
            else:
                self.handle_failed_attempt()
                if not self.is_locked:
                    if self.confirm_btn and self.confirm_btn.winfo_exists():
                        self.confirm_btn.config(state='normal', text="Confirm")
                    if self.password_entry and self.password_entry.winfo_exists():
                        self.password_entry.delete(0, tk.END)
                        self.password_entry.focus_set()
                else:
                    if self.confirm_btn and self.confirm_btn.winfo_exists():
                        self.confirm_btn.config(state='disabled', text="Confirm")
                
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during verification:\n{str(e)}")
            if self.confirm_btn and self.confirm_btn.winfo_exists():
                self.confirm_btn.config(state='normal', text="Confirm")
            if self.password_entry and self.password_entry.winfo_exists():
                self.password_entry.delete(0, tk.END)
                self.password_entry.focus_set()
    
    def on_cancel(self):
        """Handle cancel button click - treat as failed attempt"""
        if not self.is_locked:
            self.handle_failed_attempt()
    
    def monitor_inactivity(self):
        """Monitor system inactivity in background"""
        while self.monitoring:
            try:
                if self.auth_success and self.window_destroyed:
                    idle_minutes = get_idle_duration()
                    
                    if idle_minutes >= self.inactivity_minutes:
                        # Reset authentication status
                        self.auth_success = False
                        self.window_destroyed = False
                        self.failed_attempts = 0
                        
                        # Create new window in a separate thread
                        def create_new_window():
                            # Create a new Tk instance in a separate thread
                            self.overlay_root = None
                            self.create_window()
                            if self.overlay_root:
                                self.overlay_root.mainloop()
                        
                        window_thread = threading.Thread(target=create_new_window, daemon=True)
                        window_thread.start()
                        
                        # Wait a bit to avoid multiple recreations
                        time.sleep(2)
                
                time.sleep(1)
            except Exception as e:
                print(f"Inactivity monitor error: {e}")
                time.sleep(1)
    
    def create_window(self):
        """Create the main authentication window with portable-sized dialog"""
        # Create the full-screen overlay window
        self.overlay_root = tk.Tk()
        self.overlay_root.title("Server Authentication Required")
        
        # Get screen dimensions
        screen_width = self.overlay_root.winfo_screenwidth()
        screen_height = self.overlay_root.winfo_screenheight()
        
        # Set full screen geometry
        self.overlay_root.geometry(f"{screen_width}x{screen_height}+0+0")
        
        # Make window fullscreen and always on top
        self.overlay_root.attributes('-fullscreen', True)
        self.overlay_root.attributes('-topmost', True)
        self.overlay_root.attributes('-alpha', 0.95)
        
        # Remove window decorations
        self.overlay_root.overrideredirect(True)
        
        # Set background color
        self.overlay_root.configure(bg='#000000')
        
        # Prevent closing
        self.overlay_root.protocol('WM_DELETE_WINDOW', lambda: None)
        
        # Create main frame
        overlay_frame = tk.Frame(self.overlay_root, bg='#000000')
        overlay_frame.place(x=0, y=0, width=screen_width, height=screen_height)
        
        # PORTABLE DIALOG SIZE - much smaller and compact
        dialog_width = 380
        dialog_height = 300
        x_center = (screen_width - dialog_width) // 2
        y_center = (screen_height - dialog_height) // 2
        
        # Create dialog frame with portable size
        self.dialog_frame = tk.Frame(
            self.overlay_root,
            bg='#2c3e50',
            width=dialog_width,
            height=dialog_height,
            highlightbackground='#34495e',
            highlightthickness=1
        )
        self.dialog_frame.place(x=x_center, y=y_center, width=dialog_width, height=dialog_height)
        self.dialog_frame.pack_propagate(False)
        
        # Inner padding frame with less padding
        inner_frame = tk.Frame(self.dialog_frame, bg='#2c3e50', padx=25, pady=15)
        inner_frame.pack(fill='both', expand=True)
        
        # Title - smaller font
        title_label = tk.Label(
            inner_frame,
            text="🔐 Server Authentication",
            font=('Arial', 12, 'bold'),
            bg='#2c3e50',
            fg='#ecf0f1'
        )
        title_label.pack(pady=(0, 15))
        
        # Subtitle with inactivity info - smaller font
        inactivity_text = f"Inactivity: {self.inactivity_value} {self.inactivity_unit}"
        subtitle_label = tk.Label(
            inner_frame,
            text=inactivity_text,
            font=('Arial', 8),
            bg='#2c3e50',
            fg='#bdc3c7'
        )
        subtitle_label.pack(pady=(0, 10))
        
        # Password Label
        pass_label = tk.Label(
            inner_frame,
            text="Server Password:",
            font=('Arial', 9),
            bg='#2c3e50',
            fg='#ecf0f1',
            anchor='w'
        )
        pass_label.pack(fill='x', pady=(0, 3))
        
        # Password Entry - smaller
        self.password_entry = tk.Entry(
            inner_frame,
            show="●",
            font=('Arial', 10),
            bg='#34495e',
            fg='#ecf0f1',
            insertbackground='#ecf0f1',
            relief='flat',
            bd=3
        )
        self.password_entry.pack(fill='x', ipady=4)
        self.password_entry.focus_set()
        
        # Lockout label
        self.lockout_label = tk.Label(
            inner_frame,
            text="",
            font=('Arial', 8),
            bg='#2c3e50',
            fg='#e74c3c',
            wraplength=300
        )
        self.lockout_label.pack(fill='x', pady=(5, 0))
        
        # Button Frame
        button_frame = tk.Frame(inner_frame, bg='#2c3e50')
        button_frame.pack(fill='x', pady=(15, 0))
        
        # Cancel Button
        self.cancel_btn = tk.Button(
            button_frame,
            text="Cancel",
            font=('Arial', 9),
            bg='#e74c3c',
            fg='white',
            activebackground='#c0392b',
            activeforeground='white',
            relief='flat',
            padx=15,
            pady=4,
            cursor='hand2',
            command=self.on_cancel
        )
        self.cancel_btn.pack(side='left', padx=(0, 5))
        
        # Confirm Button
        self.confirm_btn = tk.Button(
            button_frame,
            text="Confirm",
            font=('Arial', 9, 'bold'),
            bg='#27ae60',
            fg='white',
            activebackground='#229954',
            activeforeground='white',
            relief='flat',
            padx=15,
            pady=4,
            cursor='hand2',
            command=self.on_confirm
        )
        self.confirm_btn.pack(side='right', padx=(5, 0))
        
        # Status Bar - smaller font
        self.status_label = tk.Label(
            self.dialog_frame,
            text=f"Enter ↵ | Esc ✗ | Attempts: {self.max_attempts}",
            font=('Arial', 7),
            bg='#34495e',
            fg='#95a5a6',
            pady=2
        )
        self.status_label.pack(fill='x', side='bottom')
        
        def on_key_press(event):
            if event.keysym == 'Return':
                self.on_confirm()
            elif event.keysym == 'Escape':
                self.on_cancel()
        
        # Bind keyboard events
        self.overlay_root.bind('<Key>', on_key_press)
        
        def maintain_focus():
            try:
                if (self.overlay_root and self.overlay_root.winfo_exists() and 
                    self.password_entry and self.password_entry.winfo_exists() and
                    not self.is_locked):
                    current_focus = self.overlay_root.focus_get()
                    if current_focus != self.password_entry:
                        self.overlay_root.lift()
                        self.overlay_root.attributes('-topmost', True)
                        self.password_entry.focus_set()
            except:
                pass
            if self.overlay_root and self.overlay_root.winfo_exists():
                self.after_id = self.overlay_root.after(200, maintain_focus)
        
        maintain_focus()
        self.window_destroyed = False
    
    def run(self):
        """Run the authentication window"""
        self.create_window()
        if self.overlay_root:
            self.overlay_root.mainloop()
        return {'authenticated': self.auth_success, 'server_id': self.server_id}

def display_pass_window_and_verify(inactivity_config="1 hour"):
    """
    Display password window with lockout mechanism and inactivity monitoring
    
    Args:
        inactivity_config: String like "1 minute", "2 hours", "3 days", "1 month"
    
    Returns:
        dict: Authentication result
    """
    auth_window = ServerAuthWindow(inactivity_config)
    return auth_window.run()

if __name__ == "__main__":
    result = display_pass_window_and_verify(inactivity_config="1 minute")
    
    if result['authenticated']:
        print(f"✓ Access granted! Server ID: {result['server_id']}")
        print("Authentication successful! Window destroyed.")
        print("Will reappear after 1 minute(s) of inactivity.")
        print("Script running in background. Press Ctrl+C to terminate.")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScript terminated by user.")
    else:
        print(f"✗ Access denied: Max attempts exceeded.")
        print("Please restart the application.")
        