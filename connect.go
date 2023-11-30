package main

import (
	"fmt"
	"github.com/gocql/gocql"
)

var session *gocql.Session

func init() {
	// Initialize Cassandra session
	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.Keyspace = "school"
	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
}

type Student struct {
	StudentID gocql.UUID
	Name      string
	ClassIDs  []gocql.UUID
}

type Class struct {
	ClassID   gocql.UUID
	ClassName string
}

// Insert a new student
func insertStudent(student Student) {
	if err := session.Query(
		"INSERT INTO students (student_id, name, class_ids) VALUES (?, ?, ?)",
		student.StudentID, student.Name, student.ClassIDs).Exec(); err != nil {
		panic(err)
	}
}

// Insert a new class
func insertClass(class Class) {
	if err := session.Query(
		"INSERT INTO classes (class_id, class_name) VALUES (?, ?)",
		class.ClassID, class.ClassName).Exec(); err != nil {
		panic(err)
	}
}

// Retrieve a student by ID
func getStudentByID(studentID gocql.UUID) Student {
	var student Student
	if err := session.Query(
		"SELECT student_id, name, class_ids FROM students WHERE student_id = ?",
		studentID).Scan(&student.StudentID, &student.Name, &student.ClassIDs); err != nil {
		panic(err)
	}
	return student
}

// Retrieve a class by ID
func getClassByID(classID gocql.UUID) Class {
	var class Class
	if err := session.Query(
		"SELECT class_id, class_name FROM classes WHERE class_id = ?",
		classID).Scan(&class.ClassID, &class.ClassName); err != nil {
		panic(err)
	}
	return class
}

func main() {
	// Example usage
	classID := gocql.TimeUUID()
	studentID := gocql.TimeUUID()

	// Insert classes
	insertClass(Class{ClassID: classID, ClassName: "Math"})

	// Insert a student with the associated class ID
	insertStudent(Student{
		StudentID: studentID,
		Name:      "John Doe",
		ClassIDs:  []gocql.UUID{classID},
	})

	// Retrieve student and class information
	retrievedStudent := getStudentByID(studentID)
	fmt.Printf("Student: %+v\n", retrievedStudent)

	for _, classID := range retrievedStudent.ClassIDs {
		retrievedClass := getClassByID(classID)
		fmt.Printf("Class: %+v\n", retrievedClass)
	}
}
