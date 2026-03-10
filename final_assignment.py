import pandas as pd
import numpy as np

print("=== TASK 1: Data Preparation and Missing Value Handling ===")

# -----------------------------
# Create Students DataFrame
# -----------------------------
students_data = {
    'student_id': [101,102,103,104,105,106,107],
    'name': ['Alice','Bob',None,'David','Emma','Frank','Grace'],
    'email': ['alice@email.com','bob@email.com','charlie@email.com',
              None,'emma@email.com','frank@email.com','grace@email.com'],
    'city': ['Mumbai','Delhi','Bangalore','Mumbai',None,'Chennai','Delhi']
}

students_df = pd.DataFrame(students_data)

print("\nOriginal Students DataFrame:")
print(students_df)

# -----------------------------
# Missing Value Analysis
# -----------------------------
print("\nNull Value Analysis:")
null_counts = students_df.isnull().sum()
total_rows = len(students_df)

for col in students_df.columns:
    pct = (null_counts[col] / total_rows) * 100
    print(f"Column {col}, Nulls: {null_counts[col]} ({pct:.2f}%)")

# -----------------------------
# Handle Missing Values
# -----------------------------
students_df['city'] = students_df['city'].fillna("Unknown")
students_df = students_df.dropna(subset=['name'])

print("\nCleaned Students DataFrame:")
print(students_df)

# -----------------------------
# Create Enrollments DataFrame
# -----------------------------
enrollments_data = {
    'student_id': [101,102,103,105,108,109],
    'course_name': ['Python','Data Science','Python',
                    'Machine Learning','AI','Python'],
    'enrollment_date': ['2024-01-15','2024-01-20','2024-02-01',
                        '2024-02-10','2024-02-15','2024-03-01']
}

enrollments_df = pd.DataFrame(enrollments_data)

# -----------------------------
# Create Scores DataFrame
# -----------------------------
scores_data = {
    'student_id': [101,102,104,105,106],
    'exam_score': [85,92,78,88,95]
}

scores_df = pd.DataFrame(scores_data)

# =====================================================
# TASK 2: Multiple Join Operations
# =====================================================

print("\n=== TASK 2: Join Operations ===")

# INNER JOIN
inner_join = pd.merge(students_df, enrollments_df, on='student_id', how='inner')
print("\nInner Join Result:")
print(inner_join)
print("Rows:", len(inner_join))

excluded = set(students_df.student_id) - set(enrollments_df.student_id)
print("Excluded students:", excluded)

# LEFT JOIN
left_join = pd.merge(students_df, enrollments_df, on='student_id', how='left')
print("\nLeft Join Result:")
print(left_join)
print("Rows:", len(left_join))

missing_courses = left_join[left_join['course_name'].isnull()]['student_id'].tolist()
print("Students with null course_name:", missing_courses)

# RIGHT JOIN
right_join = pd.merge(students_df, enrollments_df, on='student_id', how='right')
print("\nRight Join Result:")
print(right_join)
print("Rows:", len(right_join))

missing_names = right_join[right_join['name'].isnull()]['student_id'].tolist()
print("Student IDs without names:", missing_names)

# FULL OUTER JOIN
outer_join = pd.merge(students_df, enrollments_df, on='student_id', how='outer')
print("\nFull Outer Join Result:")
print(outer_join)
print("Rows:", len(outer_join))

print("\nRows with missing values:")
print(outer_join[outer_join.isnull().any(axis=1)])

# Indicator column
outer_indicator = pd.merge(
    students_df,
    enrollments_df,
    on='student_id',
    how='outer',
    indicator=True
)

print("\nMerge Source Distribution:")
print(outer_indicator['_merge'].value_counts())

# =====================================================
# TASK 3: Lookup Operation and Automation
# =====================================================

print("\n=== TASK 3: Lookup and Automation ===")

# -----------------------------
# Lookup using map()
# -----------------------------
score_lookup = dict(zip(scores_df.student_id, scores_df.exam_score))
students_df['exam_score'] = students_df['student_id'].map(score_lookup)

print("\nLookup Operation Result:")
print(students_df[['student_id','name','exam_score']])

# -----------------------------
# Merge alternative
# -----------------------------
merge_scores = pd.merge(
    students_df.drop(columns=['exam_score']),
    scores_df,
    on='student_id',
    how='left'
)

print("\nMerge Result:")
print(merge_scores[['student_id','name','exam_score']])

# -----------------------------
# Automation Function
# -----------------------------
def automate_join(df1, df2, join_type, key_column):
    result = pd.merge(df1, df2, how=join_type, on=key_column)

    print("\nAutomation Function Test")
    print("Join Type:", join_type)
    print("Rows in Result:", len(result))
    print("\nResult Preview:")
    print(result.head())

    return result

# Test automation
auto_result = automate_join(students_df, enrollments_df, "inner", "student_id")