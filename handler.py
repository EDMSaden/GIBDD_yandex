import _sqlite3 as sq
import io
import ydb
import os

from aiogram import types, Dispatcher
from create_bot import dp, bot, count_row_examination_paper, label, examination_paper
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton 
from create_bot import session


async def commands_start(message: types.Message):
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT EXISTS  
    (SELECT user_id FROM users
    WHERE user_id = {message.from_user.id})""",commit_tx=True)

    for row in result_sets[0].rows:
        user_id = row.column0

    if user_id:
        print(f'Пользователь {user_id} есть в таблице')
    else:
        session.transaction().execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        UPSERT INTO users (user_id, done_paper, favourites)
        VALUES({message.from_user.id}, ';', ';');
        """,
        commit_tx=True,
    )
        print('Новый пользователь зарегестриврован ',message.from_user.id)

    main_photo_id = await bot.send_photo(message.from_user.id, label)
    main_photo_id = main_photo_id.message_id

    #Есть ли в избранном вопросы?
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT EXISTS (SELECT favourites FROM users
    WHERE user_id = {message.from_user.id} 
    and favourites like "%p%")
    """,commit_tx=True)

    for row in result_sets[0].rows:
        favourites_bool = row.column0
 
    #Пройденные билеты
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT `done_paper` 
    FROM `users` 
    WHERE `user_id` = {message.from_user.id}
    """,commit_tx=True)
    for row in result_sets[0].rows:
        done_paper = row.done_paper.decode(encoding='UTF-8')
        done_paper = done_paper.split(';')
    
    #Создание кнопок билетов 
    kb = InlineKeyboardMarkup()
    [kb.insert((InlineKeyboardButton(text=f'Билет {i+1} ✅' if f'examination_paper_{i+1}' in done_paper else f'Билет {i+1}', callback_data = f'Билет №{i}')))for i in range(len(examination_paper))]

    if favourites_bool:
        kb.add(InlineKeyboardButton(text='Избранное', callback_data='Избранное'))

    main_msg_id = await bot.send_message(chat_id=message.from_user.id,text='ПДД', reply_markup=kb)
    main_msg_id = main_msg_id.message_id

    session.transaction().execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        UPDATE `users`
        SET `main_msg_id` = {main_msg_id}, `main_photo_id` = {main_photo_id}, `iteration_position` = 1
        WHERE `user_id` = {message.from_user.id};
        """,
        commit_tx=True,
    )

async def answer_menager(callback: types.CallbackQuery):
    print(callback.data)

    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT `iteration_position`, `main_msg_id`,`examination_paper` 
    FROM `users` 
    WHERE `user_id` = {callback.from_user.id}
    """,commit_tx=True)
    
    for row in result_sets[0].rows:
        iteration_position = row.iteration_position
        main_msg_id = row.main_msg_id
        _examination_paper = examination_paper[row.examination_paper]

    #Есть ли в избранном вопросы?
    if 'Избранное' in callback.data:

        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        SELECT EXISTS (SELECT favourites FROM users
        WHERE user_id = {callback.from_user.id}
        and favourites like "%p%")
        """,commit_tx=True)

        for row in result_sets[0].rows:
            list_empty = row.column0
            print('list_empty', list_empty)

        if list_empty:
            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            SELECT `favourites`
            FROM `users` 
            WHERE `user_id` = {callback.from_user.id}
            """,commit_tx=True)
            for row in result_sets[0].rows:
                result = row.favourites.decode(encoding='UTF-8')
                result = result.split(';')
                result = result[iteration_position].split(':')
                _examination_paper = result[0]
                iteration_position = result[1]

    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT EXISTS  
    (SELECT favourites FROM users
    WHERE user_id = {callback.from_user.id}
    and favourites like "%{_examination_paper}:{iteration_position};%")""",commit_tx=True)

    for row in result_sets[0].rows:
        print("favourites_bool:", row.column0)
        favourites_bool = row.column0

    with sq.connect('GIBDD.db') as con:
        cur = con.cursor()
        cur.execute(f'SELECT quest, answers, right_answer  FROM {_examination_paper} WHERE id = {iteration_position}')
        result = cur.fetchall()
        quest = result[0][0]
        answers = result[0][1].split(';')
        right_answer = result[0][2]
        
    if 'Избранное' in callback.data:
         await bot.edit_message_text(chat_id=callback.from_user.id, text = f'Избаранное Билет № {_examination_paper[-2:] if _examination_paper[-2] != "0" else _examination_paper[-1]} Вопрос № {iteration_position}\n\n{quest}', message_id=main_msg_id)
    else:
        await bot.edit_message_text(chat_id=callback.from_user.id, text = f'Билет № {_examination_paper[-2:] if _examination_paper[-2] != "0" else _examination_paper[-1]} Вопрос № {iteration_position}\n\n{quest}', message_id=main_msg_id)

    kb = InlineKeyboardMarkup()
    if callback.data.startswith('right_answer'):
        [kb.add((InlineKeyboardButton(text=f'{answers[i]} ✅' if answers[i] == right_answer else f'{answers[i]}', callback_data='Пусто' )))for i in range(len(answers))]
    elif callback.data.startswith('false_answer'):
        [kb.add((InlineKeyboardButton(text=f'{answers[i]} ❌' if answers[i] == answers[int(callback.data[13])] 
                                            else f'{answers[i]} ✅' if answers[i] == right_answer else answers[i], callback_data='Пусто')))for i in range(len(answers))]

    kb.add(InlineKeyboardButton(text='Объяснение_Избранное' if 'Избранное' in callback.data else 'Объяснение', callback_data='Объяснение_Избранное' if 'Избранное' in callback.data else 'Объяснение'))  
    kb.add(InlineKeyboardButton(text='Назад_Избранное' if 'Избранное' in callback.data else 'Назад', callback_data='Назад_Избранное' if 'Избранное' in callback.data else 'Назад',))
    
    if callback.data.endswith('Сердце') and favourites_bool == 0:
        session.transaction().execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        UPDATE users SET favourites = favourites || "{_examination_paper}:{iteration_position};" 
        WHERE user_id = {callback.from_user.id} and favourites 
        NOT like "%{_examination_paper}:{iteration_position}%"
        """,commit_tx=True,)

        kb.insert(InlineKeyboardButton(text='❤️', callback_data=callback.data))
        kb.insert(InlineKeyboardButton(text='Далее_Избранное' if ('Избранное' in callback.data )and list_empty else 'Далее', callback_data='Далее_Избранное' if ('Избранное' in callback.data )and list_empty else 'Далее'))
        
    elif callback.data.endswith('Сердце') and favourites_bool == 1:
        session.transaction().execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        UPDATE users 
        SET favourites  = String::ReplaceAll(favourites, '{_examination_paper}:{iteration_position};', ''),
        iteration_position = CASE iteration_position
        WHEN 1 THEN 1 
        ELSE iteration_position - 1
        END
        WHERE user_id = {callback.from_user.id}
        """,commit_tx=True,)

        kb.insert(InlineKeyboardButton(text='🖤', callback_data=callback.data))
        kb.insert(InlineKeyboardButton(text='Далее_Избранное_-1' if ('Избранное' in callback.data )and list_empty else 'Далее', callback_data='Далее_Избранное_-1' if ('Избранное' in callback.data )and list_empty else 'Далее'))
    else:
        kb.insert(InlineKeyboardButton(text='🖤'if favourites_bool == 0 else '❤️', callback_data=f'{callback.data} Сердце'))
        kb.insert(InlineKeyboardButton(text='Далее_Избранное' if ('Избранное' in callback.data )and list_empty else 'Далее', callback_data='Далее_Избранное' if ('Избранное' in callback.data )and list_empty else 'Далее'))


    await callback.message.edit_reply_markup(kb)

async def explanation_menager(callback: types.CallbackQuery):
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT `iteration_position`, `examination_paper` 
    FROM `users` 
    WHERE `user_id` = {callback.from_user.id}
    """,commit_tx=True)

    for row in result_sets[0].rows:
        iteration_position = row.iteration_position
        _examination_paper = examination_paper[row.examination_paper]

    if 'Избранное' in callback.data:
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        SELECT `favourites` 
        FROM `users` 
        WHERE `user_id` = {callback.from_user.id}
        """,commit_tx=True)

        for row in result_sets[0].rows:
            result = row.favourites.decode(encoding='UTF-8')
            result = result.split(';')
            result = result[iteration_position].split(':')
            _examination_paper = result[0]
            iteration_position = result[1]

    with sq.connect('GIBDD.db') as con:
        cur = con.cursor()
        cur.execute(f'SELECT explanation  FROM {_examination_paper} WHERE id = {iteration_position}')
        result = cur.fetchall()
        explanation = result[0][0]
    explanation_msg = await bot.send_message(chat_id=callback.from_user.id, text=explanation)
    explanation_msg = explanation_msg.message_id

    session.transaction().execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    UPDATE `users`
    SET `explanation_msg` = {explanation_msg}
    WHERE `user_id` = {callback.from_user.id};
    """,commit_tx=True,)

async def start_test(callback: types.CallbackQuery):

    #Записываем выбор билетов
    if callback.data.startswith('Билет'):
        _examination_paper = examination_paper[int(callback.data[-2:]) if callback.data[-2] != "№" else int(callback.data[-1])]
        session.transaction().execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        UPDATE `users`
        SET `examination_paper` = {int(callback.data[-2:]) if callback.data[-2] != "№" else int(callback.data[-1])}
        WHERE `user_id` = {callback.from_user.id};
        """,commit_tx=True,)
           
    elif callback.data.startswith('Далее') or callback.data.startswith('Назад'):
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        SELECT `iteration_position`, `main_msg_id`, `main_photo_id`, `examination_paper` , `explanation_msg` 
        FROM `users` 
        WHERE `user_id` = {callback.from_user.id}
        """,commit_tx=True)

        for row in result_sets[0].rows:
            iteration_position = row.iteration_position
            main_msg_id = row.main_msg_id
            main_photo_id = row.main_photo_id
            _examination_paper = examination_paper[row.examination_paper]
            explanation_msg = row.explanation_msg

        #Удаление пояснения
        try:     await bot.delete_message(chat_id=callback.from_user.id, message_id=explanation_msg)
        except : print('MessageToDeleteNotFound')


        if callback.data == 'Далее_Избранное':
            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            Select length(favourites) - length(String::ReplaceAll(favourites, ';', '')) 
            from `users`
            WHERE `user_id` = {callback.from_user.id}
            """,commit_tx=True)

            for row in result_sets[0].rows:
                number_of_favorites = row.column0 - 1
                print('В избранном :',number_of_favorites)
                
            session.transaction().execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            UPDATE users SET iteration_position = CASE iteration_position
            WHEN {number_of_favorites} THEN 1 
            ELSE iteration_position + 1
            END
            WHERE user_id = {callback.from_user.id};
             """, commit_tx=True,)

        elif callback.data == ('Далее'):
            session.transaction().execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            UPDATE users SET iteration_position = CASE iteration_position
            WHEN {count_row_examination_paper} THEN 1 
            ELSE iteration_position + 1
            END
            WHERE user_id = {callback.from_user.id}
            """,commit_tx=True,)
            
        elif callback.data.startswith('Назад'):

            session.transaction().execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            UPDATE users SET iteration_position = CASE iteration_position
            WHEN 1 THEN 1 
            ELSE iteration_position - 1
            END
            WHERE user_id = {callback.from_user.id}
            """,commit_tx=True,)

        #Пройденный билет
        if callback.data == ('Далее') and iteration_position == count_row_examination_paper:
            session.transaction().execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            UPDATE users SET done_paper = done_paper || '{_examination_paper};'
            WHERE user_id = {callback.from_user.id}
            AND done_paper not like '%{_examination_paper}%'
            """,commit_tx=True,)
            
        #Конец теста
        if callback.data == ('Далее') and iteration_position == count_row_examination_paper or callback.data.startswith('Назад') and iteration_position == 1 or callback.data == 'Далее_Избранное' and iteration_position == number_of_favorites or iteration_position == 1 and callback.data == 'Далее_Избранное_-1' :
            image = io.BytesIO(label)
            try: 
                await bot.edit_message_media(chat_id=callback.from_user.id, message_id=main_photo_id, media=types.InputMediaPhoto(image))
            except:
                pass
            #Есть ли в избранном вопросы?

            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            SELECT EXISTS (SELECT favourites FROM users
            WHERE user_id = {callback.from_user.id} 
            and favourites like "%p%")
            """,commit_tx=True)

            for row in result_sets[0].rows:
                favourites_bool = row.column0

            result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"""
            PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
            SELECT `done_paper` 
            FROM `users` 
            WHERE `user_id` = {callback.from_user.id}
            """,commit_tx=True)
            for row in result_sets[0].rows:
                done_paper = row.done_paper.decode(encoding='UTF-8')
                print(type(done_paper), 'done_paper', done_paper)
                done_paper = done_paper.split(';')

            kb = InlineKeyboardMarkup()
            [kb.insert((InlineKeyboardButton(text=f'Билет {i+1} ✅' if f'examination_paper_{i+1}' in done_paper else f'Билет {i+1}', callback_data = f'Билет №{i}')))for i in range(len(examination_paper))]
            
            if favourites_bool:
                kb.add(InlineKeyboardButton(text='Избранное', callback_data='Избранное'))

            await bot.edit_message_text(chat_id=callback.from_user.id,text='ПДД', message_id=main_msg_id, reply_markup=kb)

            return None
     
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
    f"""
    PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
    SELECT `iteration_position`, `main_msg_id`, `main_photo_id`, `examination_paper` 
    FROM `users` 
    WHERE `user_id` = {callback.from_user.id}
    """,commit_tx=True)

    for row in result_sets[0].rows:
        print("iteration_position:", type(row.iteration_position), "main_msg_id:", type(row.main_msg_id),
        "main_photo_id:", type(row.main_photo_id), "examination_paper:", type(row.examination_paper))
        iteration_position = row.iteration_position
        main_msg_id = row.main_msg_id
        main_photo_id = row.main_photo_id
        _examination_paper = examination_paper[row.examination_paper]
    
    if 'Избранное' in callback.data:
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        f"""
        PRAGMA TablePathPrefix("{os.getenv('YDB_DATABASE')}");
        SELECT `favourites` 
        FROM `users` 
        WHERE `user_id` = {callback.from_user.id}
        """,commit_tx=True)

        for row in result_sets[0].rows:
            result = row.favourites.decode(encoding='UTF-8')
            result = result.split(';')
            if result:
                result = result[iteration_position].split(':')
                _examination_paper = result[0]
                iteration_position = result[1]

    with sq.connect('GIBDD.db') as con:
        print(_examination_paper)
        cur = con.cursor()
        cur.execute(f'SELECT quest,answers,right_answer,image FROM {_examination_paper} WHERE id = {iteration_position}')
        result = cur.fetchall()
        quest = result[0][0]
        answers = result[0][1].split(';')
        right_answer = result[0][2]
        image = result[0][3]

    image = io.BytesIO(image)
    
    try:   
        await bot.edit_message_media(chat_id=callback.from_user.id, message_id=main_photo_id, media=types.InputMediaPhoto(image))
    except:
        pass
    
    if 'Избранное' in callback.data:
         await bot.edit_message_text(chat_id=callback.from_user.id, text = f'Избаранное Билет № {_examination_paper[-2:] if _examination_paper[-2] != "0" else _examination_paper[-1]} Вопрос № {iteration_position}\n\n{quest}', message_id=main_msg_id)
    else:
        await bot.edit_message_text(chat_id=callback.from_user.id, text = f'Билет № {_examination_paper[-2:] if _examination_paper[-2] != "0" else _examination_paper[-1]} Вопрос № {iteration_position}\n\n{quest}', message_id=main_msg_id)

    kb = InlineKeyboardMarkup(row_width=1)
    if 'Избранное' in callback.data:
        [kb.insert((InlineKeyboardButton(text=answers[i], callback_data = 'right_answer_Избранное' if answers[i] == right_answer 
                                                                    else f'false_answer_{i}_Избранное')))for i in range(len(answers))]
    else:
        [kb.insert((InlineKeyboardButton(text=answers[i], callback_data = 'right_answer' if answers[i] == right_answer 
                                                            else f'false_answer_{i}')))for i in range(len(answers))]
    await callback.message.edit_reply_markup(kb)
    await callback.answer()