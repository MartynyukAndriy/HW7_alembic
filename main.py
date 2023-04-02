from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_one():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    SELECT s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    :return:
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    # order_by(Grade.grade.desc())
    return result


def select_two():
    """
    SELECT d.name, s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.id = 5
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 1;
    :return:
    """
    result = session.query(
        Discipline.name,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).join(Discipline)\
        .filter(Discipline.id == 5)\
        .group_by(Student.id, Discipline.name).order_by(desc('avg_grade')).limit(1).first()
    return result


def select_12():
    """
    -- Оцінки студентів у певній групі з певного предмета на останньому занятті.
    select s.id, s.fullname, g.grade, g.date_of
    from grades g
    join students s on s.id = g.student_id
    where g.discipline_id = 3 and s.group_id = 3 and g.date_of = (
        select max(date_of)
        from grades g2
        join students s2 on s2.id = g2.student_id
        where g2.discipline_id = 3 and s2.group_id = 3
    );
    :return:
    """
    subquery = (select(func.max(Grade.date_of)).join(Student).filter(and_(
        Grade.discipline_id == 3, Student.group_id == 3
    )).scalar_subquery())

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.date_of)\
                    .select_from(Grade)\
                    .join(Student)\
                    .filter(and_(
                        Grade.discipline_id == 3, Student.group_id == 3, Grade.date_of == subquery
                    )).all()
    return result


def select_3():
    '''
    Знайти середній бал у групах з певного предмета.
    SELECT gr.name, d.name, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    LEFT JOIN [groups] gr ON gr.id = s.group_id 
    WHERE d.id = 1
    GROUP BY gr.id
    ORDER BY avg_grade DESC;
    '''
    result = session.query(Discipline.name, Group.name, func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
                    .select_from(Grade)\
                    .join(Student).join(Discipline).join(Group)\
                    .filter(Discipline.id == 2)\
                    .group_by(Group.id)\
                    .order_by(desc("avg_grade")).all()
    return result


def select_4():
    """
    --4. Знайти середній бал на потоці (по всій таблиці оцінок).

    SELECT ROUND(AVG(grade), 2) as avg_grade
    FROM grades g 
    """
    result = session.query(func.round(func.avg(Grade.grade).label("avg_grade")))\
                    .select_from(Grade).all()

    return result


def select_5():
    """
    -- 5. Знайти які курси читає певний викладач.
    SELECT name
    FROM disciplines d 
    WHERE teacher_id = 2
    """

    result = session.query(Discipline.name).select_from(
        Discipline).filter(Discipline.teacher_id == 5).all()

    return result


def select_6():
    """
    -- 6. Знайти список студентів у певній групі.
    SELECT fullname
    FROM students s 
    WHERE group_id = 3
    """

    result = session.query(Student.fullname).filter(
        Student.group_id == 1).all()

    return result


def select_7():
    """
    -- 7. Знайти оцінки студентів у окремій групі з певного предмета.
    SELECT *
    FROM grades g 
    LEFT JOIN students s ON s.id = g.student_id 
    where s.group_id = 1 AND g.discipline_id = 1
    """

    result = session.query(Student.fullname, Grade.grade).select_from(Grade)\
                    .join(Student)\
                    .filter(and_(Student.group_id == 1, Grade.discipline_id == 1)).all()

    return result


def select_8():
    """
    -- 8. Знайти середній бал, який ставить певний викладач зі своїх предметів.
    SELECT d.name, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g 
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.teacher_id = 2
    GROUP BY g.discipline_id
    """

    result = session.query(Discipline.name, func.round(func.avg(Grade.grade), 2).label("avg_grade"))\
                    .select_from(Grade)\
                    .join(Discipline)\
                    .filter(Discipline.teacher_id == 1)\
                    .group_by(Discipline.id).all()

    return result


def select_9():
    """
    -- 9. Знайти список курсів, які відвідує студент.
    SELECT d.name
    FROM grades g 
    JOIN disciplines d ON d.id = g.discipline_id 
    WHERE  g.student_id = 2
    GROUP BY g.discipline_id
    """

    result = session.query(Discipline.name).select_from(Grade)\
                    .join(Discipline)\
                    .filter(Grade.student_id == 2)\
                    .group_by(Discipline.id).all()

    return result


def select_10():
    """
    -- 10. Список курсів, які певному студенту читає певний викладач.
    SELECT d.name
    FROM grades g 
    LEFT JOIN disciplines d ON d.id = g.discipline_id 
    WHERE  d.teacher_id = 2 AND g.student_id = 2
    """

    result = session.query(Discipline.name).select_from(Grade)\
                    .join(Discipline)\
                    .filter(and_(Discipline.teacher_id == 2, Grade.student_id == 2))\
                    .all()
    return result


if __name__ == '__main__':
    # print(select_one())
    # print(select_two())
    # print(select_12())
    # print(select_3())
    # print(select_4())
    # print(select_5())
    # print(select_6())
    # print(select_7())
    # print(select_8())
    # print(select_9())
    # print(select_10())
    pass
