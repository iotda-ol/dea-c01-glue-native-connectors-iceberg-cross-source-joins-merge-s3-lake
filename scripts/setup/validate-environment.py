#!/usr/bin/env python3
"""
Environment Validation Script
Validates that all prerequisites are properly configured
"""
import sys
import subprocess
import json


def check_aws_credentials():
    """Check if AWS credentials are configured"""
    try:
        result = subprocess.run(['aws', 'sts', 'get-caller-identity'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            identity = json.loads(result.stdout)
            print(f"✓ AWS credentials configured for account: {identity['Account']}")
            return True
        else:
            print("✗ AWS credentials not configured")
            return False
    except Exception as e:
        print(f"✗ Error checking AWS credentials: {str(e)}")
        return False


def check_aws_cli():
    """Check if AWS CLI is installed"""
    try:
        result = subprocess.run(['aws', '--version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"✓ AWS CLI installed: {version}")
            return True
        else:
            print("✗ AWS CLI not installed")
            return False
    except Exception as e:
        print(f"✗ AWS CLI not found: {str(e)}")
        return False


def check_python_version():
    """Check Python version"""
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"✗ Python version too old: {version.major}.{version.minor}.{version.micro}")
        print("  Required: Python 3.7 or higher")
        return False


def check_required_packages():
    """Check if required Python packages are installed"""
    required = ['boto3', 'pyspark']
    all_installed = True
    
    for package in required:
        try:
            __import__(package)
            print(f"✓ {package} installed")
        except ImportError:
            print(f"✗ {package} not installed")
            all_installed = False
    
    return all_installed


def check_s3_access():
    """Check if S3 is accessible"""
    try:
        result = subprocess.run(['aws', 's3', 'ls'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✓ S3 access verified")
            return True
        else:
            print("✗ Cannot access S3")
            return False
    except Exception as e:
        print(f"✗ Error checking S3 access: {str(e)}")
        return False


def check_glue_access():
    """Check if AWS Glue is accessible"""
    try:
        result = subprocess.run(['aws', 'glue', 'list-jobs', '--max-results', '1'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✓ AWS Glue access verified")
            return True
        else:
            print("✗ Cannot access AWS Glue")
            return False
    except Exception as e:
        print(f"✗ Error checking Glue access: {str(e)}")
        return False


def main():
    """Run all validation checks"""
    print("=" * 60)
    print("Environment Validation")
    print("=" * 60)
    print()
    
    checks = [
        ("Python Version", check_python_version),
        ("AWS CLI", check_aws_cli),
        ("AWS Credentials", check_aws_credentials),
        ("Required Packages", check_required_packages),
        ("S3 Access", check_s3_access),
        ("Glue Access", check_glue_access),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\nChecking {name}...")
        results.append(check_func())
    
    print()
    print("=" * 60)
    print("Validation Summary")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Passed: {passed}/{total}")
    
    if all(results):
        print("\n✓ All checks passed! Environment is ready.")
        return 0
    else:
        print("\n✗ Some checks failed. Please resolve issues before proceeding.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
