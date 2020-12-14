from setuptools import find_packages, setup

setup(
    name='project_data_science_code',
    packages=find_packages(),
    version='0.0.2',
    description='Project-specific data science code',
    author='Aaron Fraint, AICP',
    license='MIT',
    entry_points="""
        [console_scripts]
        ucity=fy21_university_city.cli:main
        downingtown=fy21_downingtown.cli:main
    """,
)