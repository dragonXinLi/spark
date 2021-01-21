package org.apache.spark.examples;


import java.util.ArrayList;

public class Test {

    public static void main(String[] args) throws Exception {

        Student student = new Student();
        ArrayList<Student> students = new ArrayList<>();
        for (int i =0 ;i<10;i++){
            student.setName("aaa"+i);
            student.setAge(i);
            students.add(student);
        }

        System.out.println(students);

    }
}

class Student{
    private String name;
    private Integer age;

    public Student() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}