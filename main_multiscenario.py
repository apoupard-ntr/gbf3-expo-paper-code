"""
Multi-Scenario Master Script
Orchestrates the execution of all scenarios sequentially
Each scenario has its own main.py that is executed independently
"""

import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION: LIST OF SCENARIOS TO PROCESS
# ============================================================================

# Automatically discover scenarios from the multiscenarios folder
MULTISCENARIOS_DIR = "multiscenarios"

def discover_scenarios():
    """
    Automatically discover all scenario folders in the multiscenarios directory.
    Returns a sorted list of scenario names.
    """
    if not os.path.isdir(MULTISCENARIOS_DIR):
        print(f"✗ ERROR: Directory '{MULTISCENARIOS_DIR}' not found!")
        return []
    
    scenarios = []
    try:
        for item in os.listdir(MULTISCENARIOS_DIR):
            item_path = os.path.join(MULTISCENARIOS_DIR, item)
            # Only include directories
            if os.path.isdir(item_path):
                scenarios.append(item)
        scenarios.sort()
    except Exception as e:
        print(f"✗ ERROR discovering scenarios: {e}")
        return []
    
    return scenarios

SCENARIOS = discover_scenarios()

# ============================================================================
# EXECUTION TRACKING
# ============================================================================

class ScenarioRunner:
    def __init__(self, scenarios):
        self.scenarios = scenarios
        self.results = {}
        self.start_time = None
        self.end_time = None
    
    def run_scenario(self, scenario_name):
        """
        Execute the main.py script for a given scenario.
        Scenarios are located in the multiscenarios folder.
        """
        scenario_dir = os.path.join(MULTISCENARIOS_DIR, scenario_name)
        main_script = os.path.join(scenario_dir, "main.py")
        
        print(f"\n{'='*70}")
        print(f"SCENARIO: {scenario_name}")
        print(f"Script: {main_script}")
        print(f"{'='*70}")
        
        # Check if main.py exists
        if not os.path.exists(main_script):
            print(f"✗ ERROR: {main_script} not found!")
            self.results[scenario_name] = {
                'status': 'FAILED',
                'error': f"Script not found: {main_script}"
            }
            return False
        
        # Check if final_result_Shen.csv already exists (skip if present)
        final_result_csv = os.path.join(scenario_dir, "final_result_Shen.csv")
        if os.path.exists(final_result_csv):
            print(f"✓ Result file already exists: {final_result_csv}")
            print(f"  Skipping {scenario_name} (already processed)")
            self.results[scenario_name] = {
                'status': 'SKIPPED',
                'reason': 'Result file already exists'
            }
            return True
        
        # Run the scenario script
        try:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting execution...\n")
            
            # Execute the main.py script from the project root directory
            # Pass the scenario name as argument
            result = subprocess.run(
                [sys.executable, main_script, scenario_name],
                cwd=os.getcwd(),
                capture_output=False,
                text=True
            )
            
            if result.returncode == 0:
                print(f"\n✓ {scenario_name} completed successfully")
                self.results[scenario_name] = {
                    'status': 'SUCCESS',
                    'returncode': 0
                }
                return True
            else:
                print(f"\n✗ {scenario_name} failed with return code {result.returncode}")
                self.results[scenario_name] = {
                    'status': 'FAILED',
                    'returncode': result.returncode
                }
                return False
                
        except Exception as e:
            print(f"\n✗ ERROR executing {scenario_name}: {e}")
            self.results[scenario_name] = {
                'status': 'ERROR',
                'error': str(e)
            }
            return False
    
    def run_all_scenarios(self):
        """
        Execute all scenarios in sequence
        """
        print("\n" + "="*70)
        print("MULTI-SCENARIO EXECUTION")
        print("="*70)
        print(f"Scenarios to process: {len(self.scenarios)}")
        for i, scenario in enumerate(self.scenarios, 1):
            print(f"  {i}. {scenario}")
        
        self.start_time = datetime.now()
        print(f"\nStart time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Execute each scenario
        for i, scenario_name in enumerate(self.scenarios, 1):
            print(f"\n[{i}/{len(self.scenarios)}] Processing {scenario_name}...")
            self.run_scenario(scenario_name)
        
        self.end_time = datetime.now()
        self.print_summary()
    
    def print_summary(self):
        """
        Print execution summary
        """
        print("\n" + "="*70)
        print("EXECUTION SUMMARY")
        print("="*70)
        
        successful = sum(1 for r in self.results.values() if r['status'] == 'SUCCESS')
        skipped = sum(1 for r in self.results.values() if r['status'] == 'SKIPPED')
        failed = sum(1 for r in self.results.values() if r['status'] in ['FAILED', 'ERROR'])
        
        for scenario_name, result in self.results.items():
            if result['status'] == 'SUCCESS':
                status_symbol = "✓"
            elif result['status'] == 'SKIPPED':
                status_symbol = "⊘"
            else:
                status_symbol = "✗"
            
            print(f"{status_symbol} {scenario_name}: {result['status']}")
            if 'error' in result:
                print(f"   └─ {result['error']}")
            if 'reason' in result:
                print(f"   └─ {result['reason']}")
        
        print(f"\n{'='*70}")
        print(f"Total scenarios: {len(self.scenarios)}")
        print(f"Successful: {successful}")
        print(f"Skipped: {skipped}")
        print(f"Failed: {failed}")
        print(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        duration = self.end_time - self.start_time
        print(f"Duration: {duration}")
        print(f"{'='*70}")
        
        if failed == 0:
            if skipped > 0:
                print(f"\n✓ COMPLETED ({successful} processed, {skipped} skipped)")
            else:
                print("\n✓ ALL SCENARIOS COMPLETED SUCCESSFULLY")
            return 0
        else:
            print(f"\n⚠ {failed} scenario(s) failed")
            return 1

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main entry point for multi-scenario execution
    """
    if not SCENARIOS:
        print("✗ ERROR: No scenarios configured!")
        print("Please add scenario names to the SCENARIOS list in this file.")
        return 1
    
    runner = ScenarioRunner(SCENARIOS)
    exit_code = runner.run_all_scenarios()
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
