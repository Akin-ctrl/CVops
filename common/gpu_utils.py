"""
GPU detection and monitoring utilities for CVops services.
Provides GPU availability checks, device selection, and metrics collection.
"""

import logging
import os
import subprocess


def check_cuda_available():
    """
    Check if CUDA is available via PyTorch.
    
    Returns:
        bool: True if CUDA is available, False otherwise
    """
    try:
        import torch
        return torch.cuda.is_available()
    except ImportError:
        logging.warning("PyTorch not installed, assuming no CUDA")
        return False
    except Exception as e:
        logging.error(f"Error checking CUDA availability: {e}")
        return False


def get_gpu_info():
    """
    Get detailed GPU information.
    
    Returns:
        dict: GPU information including name, memory, CUDA version, etc.
    """
    info = {
        'cuda_available': False,
        'gpu_count': 0,
        'devices': []
    }
    
    try:
        import torch
        
        if not torch.cuda.is_available():
            return info
        
        info['cuda_available'] = True
        info['gpu_count'] = torch.cuda.device_count()
        info['cuda_version'] = torch.version.cuda
        info['pytorch_version'] = torch.__version__
        
        # Get info for each GPU
        for i in range(torch.cuda.device_count()):
            device_info = {
                'id': i,
                'name': torch.cuda.get_device_name(i),
                'capability': torch.cuda.get_device_capability(i),
                'total_memory_gb': round(torch.cuda.get_device_properties(i).total_memory / 1024**3, 2)
            }
            info['devices'].append(device_info)
        
        # Get current device if set
        if torch.cuda.is_available():
            info['current_device'] = torch.cuda.current_device()
            info['current_device_name'] = torch.cuda.get_device_name()
            
    except ImportError:
        logging.debug("PyTorch not installed")
    except Exception as e:
        logging.error(f"Error getting GPU info: {e}")
    
    return info


def get_gpu_stats():
    """
    Get current GPU utilization and memory stats using nvidia-smi.
    
    Returns:
        dict: GPU stats including utilization, memory usage, temperature
    """
    stats = {
        'available': False,
        'gpus': []
    }
    
    try:
        # Try nvidia-smi command
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=index,utilization.gpu,memory.used,memory.total,temperature.gpu',
             '--format=csv,noheader,nounits'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            stats['available'] = True
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                parts = line.split(',')
                if len(parts) >= 5:
                    gpu_stat = {
                        'id': int(parts[0].strip()),
                        'utilization_percent': float(parts[1].strip()),
                        'memory_used_mb': float(parts[2].strip()),
                        'memory_total_mb': float(parts[3].strip()),
                        'temperature_c': float(parts[4].strip())
                    }
                    gpu_stat['memory_utilization_percent'] = round(
                        (gpu_stat['memory_used_mb'] / gpu_stat['memory_total_mb']) * 100, 2
                    )
                    stats['gpus'].append(gpu_stat)
    
    except FileNotFoundError:
        logging.debug("nvidia-smi not found")
    except subprocess.TimeoutExpired:
        logging.warning("nvidia-smi timeout")
    except Exception as e:
        logging.error(f"Error getting GPU stats: {e}")
    
    return stats


def select_best_device(preferred_device='auto'):
    """
    Select the best available device (GPU or CPU) based on availability.
    
    Args:
        preferred_device: 'cuda', 'cpu', or 'auto' (default)
        
    Returns:
        str: Device string ('cuda', 'cuda:0', 'cpu')
    """
    if preferred_device == 'cpu':
        return 'cpu'
    
    if preferred_device == 'cuda' or preferred_device == 'auto':
        try:
            import torch
            
            if torch.cuda.is_available():
                # If multiple GPUs, select one with most free memory
                if torch.cuda.device_count() > 1:
                    stats = get_gpu_stats()
                    if stats['available'] and stats['gpus']:
                        # Find GPU with lowest memory usage
                        best_gpu = min(stats['gpus'], key=lambda x: x['memory_utilization_percent'])
                        device = f"cuda:{best_gpu['id']}"
                        logging.info(f"Selected {device} ({best_gpu['memory_utilization_percent']:.1f}% memory used)")
                        return device
                
                # Single GPU or couldn't get stats
                logging.info("CUDA available, using GPU")
                return 'cuda'
            else:
                if preferred_device == 'cuda':
                    logging.warning("CUDA requested but not available, falling back to CPU")
                return 'cpu'
                
        except ImportError:
            logging.warning("PyTorch not installed, using CPU")
            return 'cpu'
        except Exception as e:
            logging.error(f"Error selecting device: {e}, falling back to CPU")
            return 'cpu'
    
    # Unknown preference
    logging.warning(f"Unknown device preference '{preferred_device}', using CPU")
    return 'cpu'


def get_device_info_for_health():
    """
    Get device information formatted for health endpoint.
    
    Returns:
        dict: Device info suitable for health endpoint response
    """
    gpu_info = get_gpu_info()
    
    health_info = {
        'device_type': 'cuda' if gpu_info['cuda_available'] else 'cpu',
        'gpu_available': gpu_info['cuda_available'],
    }
    
    if gpu_info['cuda_available']:
        health_info['gpu_count'] = gpu_info['gpu_count']
        health_info['cuda_version'] = gpu_info.get('cuda_version', 'unknown')
        
        if gpu_info['devices']:
            # Just report the first/current GPU for simplicity
            device = gpu_info['devices'][0]
            health_info['gpu_name'] = device['name']
            health_info['gpu_memory_gb'] = device['total_memory_gb']
    
    return health_info


def log_device_info():
    """Log comprehensive device information at startup."""
    gpu_info = get_gpu_info()
    
    logging.info("=" * 60)
    logging.info("DEVICE INFORMATION")
    logging.info("=" * 60)
    
    if gpu_info['cuda_available']:
        logging.info(f"✓ CUDA Available: YES")
        logging.info(f"  CUDA Version: {gpu_info.get('cuda_version', 'unknown')}")
        logging.info(f"  PyTorch Version: {gpu_info.get('pytorch_version', 'unknown')}")
        logging.info(f"  GPU Count: {gpu_info['gpu_count']}")
        
        for device in gpu_info['devices']:
            logging.info(f"  GPU {device['id']}: {device['name']}")
            logging.info(f"    Memory: {device['total_memory_gb']} GB")
            logging.info(f"    Compute Capability: {device['capability']}")
        
        # Try to get current stats
        stats = get_gpu_stats()
        if stats['available'] and stats['gpus']:
            logging.info(f"  Current Status:")
            for gpu in stats['gpus']:
                logging.info(f"    GPU {gpu['id']}: {gpu['utilization_percent']:.1f}% utilized, "
                           f"{gpu['memory_utilization_percent']:.1f}% memory, "
                           f"{gpu['temperature_c']:.0f}°C")
    else:
        logging.info(f"✗ CUDA Available: NO")
        logging.info(f"  Running on CPU")
    
    logging.info("=" * 60)
