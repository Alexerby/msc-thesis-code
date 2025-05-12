from functools import wraps

def log_plot_saving(func):
    @wraps(func)
    def wrapper(*args, save_path=None, **kwargs):
        result = func(*args, save_path=save_path, **kwargs)
        if save_path:
            print(f"[✔] Plot saved to: {save_path}")
        return result
    return wrapper
