rm -rf kakaku_robot.egg-info dist
python setup.py sdist bdist_wheel
twine upload --repository pypi dist/*