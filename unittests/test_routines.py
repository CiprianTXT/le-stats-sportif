import unittest
import json
import os
from logging import getLogger
from deepdiff import DeepDiff
import sys
sys.path.append("../app/")
from task_runner import TaskRunner
from data_ingestor import DataIngestor


class TestExecRoutines(unittest.TestCase):
    def setUp(self):
        # Creating the results folder
        os.mkdir("./results")

        # Initializing the test environment
        self.task_runner = TaskRunner(
            None,
            None,
            None,
            None,
            DataIngestor("../nutrition_activity_obesity_usa_subset.csv"),
            getLogger()
        )

    def tearDown(self):
        # Deleting the results folder
        os.system("rm -rf ./results")

    def result_checker(self, test_id):
        with (
            open(f"./results/job_id_{test_id}.json", encoding="utf-8") as result_file,
            open(f"./references/ref_{test_id}.json", encoding="utf-8") as reference_file
        ):
            # Reading routine result and reference result
            result = json.load(result_file)
            reference = json.load(reference_file)

            # Comparing results using a math error bound
            diff = DeepDiff(result, reference, math_epsilon=0.01)
            self.assertTrue(not diff)

    def test_exec_states_mean(self):
        test_id = 1
        self.task_runner.exec_states_mean(
            "Percent of adults aged 18 years and older who have an overweight classification",
            test_id
        )
        self.result_checker(test_id)

    def test_exec_state_mean(self):
        test_id = 2
        self.task_runner.exec_state_mean(
            "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week",
            "Louisiana",
            test_id
        )
        self.result_checker(test_id)
#
    def test_exec_best5(self):
        test_id = 3
        self.task_runner.exec_top5(
            "Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)",
            test_id
        )
        self.result_checker(test_id)

    def test_exec_worst5(self):
        test_id = 4
        self.task_runner.exec_top5(
            "Percent of adults aged 18 years and older who have obesity",
            test_id,
            best=False
        )
        self.result_checker(test_id)

    def test_exec_global_mean(self):
        test_id = 5
        self.task_runner.exec_global_mean(
            "Percent of adults who report consuming fruit less than one time daily",
            test_id
        )
        self.result_checker(test_id)

    def test_exec_diff_from_mean(self):
        test_id = 6
        self.task_runner.exec_diff_from_mean(
            "Percent of adults who engage in no leisure-time physical activity",
            test_id
        )
        self.result_checker(test_id)

    def test_exec_state_diff_from_mean(self):
        test_id = 7
        self.task_runner.exec_state_diff_from_mean(
            "Percent of adults who report consuming vegetables less than one time daily",
            "Kentucky",
            test_id
        )
        self.result_checker(test_id)

    def test_exec_mean_by_category(self):
        test_id = 8
        self.task_runner.exec_mean_by_category(
            "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)",
            test_id
        )
        self.result_checker(test_id)

    def test_exec_state_mean_by_category(self):
        test_id = 9
        self.task_runner.exec_state_mean_by_category(
            "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week",
            "Puerto Rico",
            test_id
        )
        self.result_checker(test_id)


if __name__ == '__main__':
    unittest.main()
