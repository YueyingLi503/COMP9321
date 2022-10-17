from email.policy import default
from select import select
from tokenize import String
from attr import field
from flask import Flask, request, send_file, make_response
from flask_restx import Resource, Api, reqparse, fields
import sqlite3
import json
import requests
from datetime import datetime, timedelta
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text, desc, asc, column, select
from sqlalchemy.orm import load_only
import numpy as np
import matplotlib.pyplot as plt
import base64
import matplotlib
matplotlib.use('Agg')

app = Flask(__name__)
api = Api(app)
db = SQLAlchemy(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///app.db'

class ActorDB(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    last_update = db.Column(db.DateTime, default = datetime.now())
    name = db.Column(db.String(100), unique = True, nullable = False)
    gender = db.Column(db.String(10), nullable = True)
    country = db.Column(db.String(100),nullable = True)
    birthday = db.Column(db.DateTime, nullable = True)
    deathday = db.Column(db.DateTime, nullable = True)
    show = db.Column(db.Text, nullable = True)

    def __repr__(self):
        return f"<name:{self.name}>"
    
    def keys(self):
        return ["id", "name","country", "birthday", "deathday", "last_update", "show"]

    def __getitem__(self, item):
        return self.__getattribute__(item)

actor_post_rep = reqparse.RequestParser()
actor_post_rep.add_argument("name", type = str)

actor_list_rep = reqparse.RequestParser()
actor_list_rep.add_argument("order", type = str, default=["+id"],action="split")
actor_list_rep.add_argument("page", type = int, default = 1)
actor_list_rep.add_argument("size", type = int, default = 10)
actor_list_rep.add_argument("filter", type = str, default = ["id", "name"],action="split")

actor_stat_rep = reqparse.RequestParser()
actor_stat_rep.add_argument("format", type = str)
actor_stat_rep.add_argument("by", type = str, action = "split")

edit_model = api.model('edit payload',{"name": fields.String, "country":fields.String})

@api.route('/actors')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(201, 'Created')

class Actor(Resource):

    @api.expect(actor_list_rep)
    def get(self):
        args = actor_list_rep.parse_args()
        order_by = args["order"]
        page = args["page"]
        size = args["size"]
        filter_by = args["filter"]
        filter_by_list = []
        page = page-1
        total = int(page*size)
        #for param in filter_by:
            #filter_by_list.append(param))
        order_by_list = []
        for param in order_by:
            if param[0] == "+":
                order_by_list.append(asc(param[1:]))
                #actor_list = actorDB.query.order_by(asc(param[1:])).first()
            if param[0] == "-":
                order_by_list.append(desc(param[1:]))
                #actor_list = actorDB.query.order_by(desc(param[1:])).first()
        order_by_str = ",".join(order_by)
        filter_by_str = ",".join(filter_by)
        actor_list = ActorDB.query.order_by(*order_by_list).all()
        if len(actor_list)>= total:
            if total == 0:
                if size>len(actor_list):
                    actor_list = actor_list[0:len(actor_list)]
                else:
                    actor_list = actor_list[0:size-1]
            elif len(actor_list)-total < size and total != 0:
                actor_list = actor_list[total-1:len(actor_list)]
            else:
                actor_list = actor_list[total-1:total-1+size]
        else:
            valide_page = int(len(actor_list)/size) +1
            return{
                    "message":"page index out of range, index with page size "+str(size) +" only have "+str(valide_page)+" pages",
                },404
        actor_dict_list = [dict(actor) for actor in actor_list]
        #.with_entities(*filter_by_list)
        #select(column(filter_by_list))
        page = page+1
        if page>1:
            return_json = {"page":page,"size":size,"actors":[],
                        "_links":{"self":{"href":"http://127.0.0.1:8888/actors?"+"order="+str(order_by_str)+
                                                "&page="+str(page)+"&size="+str(size)+"&filter="+str(filter_by_str)}
                                  },"prev":{"href":"http://127.0.0.1:8888/actors?"+"order="+str(order_by_str)+
                                                "&page="+str(page-1)+"&size="+str(size)+"&filter="+str(filter_by_str)},
                                    "next":{"href":"http://127.0.0.1:8888/actors?"+"order="+str(order_by_str)+
                                                "&page="+str(page+1)+"&size="+str(size)+"&filter="+str(filter_by_str)}}
        else:
            return_json = {"page":page,"size":size,"actors":[],
                        "_links":{"self":{"href":"http://127.0.0.1:8888/actors?"+"order="+str(order_by_str)+
                                                "&page="+str(page)+"&size="+str(size)+"&filter="+str(filter_by_str)}
                                  },"next":{"href":"http://127.0.0.1:8888/actors?"+"order="+str(order_by_str)+
                                                "&page="+str(page+1)+"&size="+str(size)+"&filter="+str(filter_by_str)}}
        for actor_dict in actor_dict_list:
            re = {}
            for column, data in actor_dict.items():
                if column in filter_by:
                    if column in ["birthday", "deathday","last_update"] and data:
                        data = data.strftime("%d-%m-%Y")
                        re[column] = data
                    else:
                        re[column] = data
            return_json["actors"].append(re)
            
        '''
        for actor in actor_list:
            return_json["actors"].append({"id": actor.id,
                                        "name": actor.name,
                                        "country":actor.country})
        '''
        return return_json

    @api.expect(actor_post_rep)
    def post(self):
        args = actor_post_rep.parse_args()
        name_send = args['name']
        name_send = name_send.replace('-',' ')
        name_send = name_send.replace('_',' ')
        name_send = name_send.replace('.',' ')
        name_send = name_send.replace('?',' ')
        #name_send = ''.join([i if ord(i) < 128 else ' ' for i in name_send])
        actor_url = "https://api.tvmaze.com/search/people?q=" +name_send
        actor_request = requests.get(actor_url)
        if actor_request.status_code == 200:
            actor_text = json.loads(actor_request.text)
            if len(actor_text) > 0:
                actor_text = actor_text[0]
            else:
                return{
                    "message":str(name_send) +" not found, please try again",
                },404
            actor_info = actor_text['person']
            #return actor_info
            actor_id = actor_info['id']
            name = actor_info['name']
            if name.lower() != name_send.lower():
                return{
                    "message":str(name_send) +" not found, please try again",
            },404
            else:
                if ActorDB.query.filter_by(name=name).first():
                    return{
                    "message":str(name_send) +" already exists, please try another one",
                },400
                else:
                    country = "unknown"
                    if actor_info['country']:
                        country = actor_info['country']['name']
                    birthday = actor_info['birthday']
                    deathday = actor_info['deathday']
                    gender = actor_info['gender']
                    show_list_url = "https://api.tvmaze.com/people/"+ str(actor_id) +"/castcredits"
                    show_list_request = requests.get(show_list_url)
                    if show_list_request.status_code == 200:
                        show_list_text = json.loads(show_list_request.text)
                        #return {"showlist":show_list_text}
                        show_list = []
                        for show_info in show_list_text:
                            show_url = show_info['_links']['show']['href']
                            show_request = requests.get(show_url)
                            if show_request.status_code == 200:
                                show_text = json.loads(show_request.text)
                                show_list.append(show_text['name'])
                        show_list = ','.join(show_list)
                        if birthday:
                            birthday = datetime.strptime(birthday,"%Y-%m-%d")
                        if deathday:
                            deathday = datetime.strptime(deathday,"%Y-%m-%d")
                        actor = ActorDB(name = name, country = country,gender = gender, birthday = birthday, deathday= deathday,
                        show = show_list)
                        db.session.add(actor)
                        db.session.commit()
                        actor = ActorDB.query.filter_by(name=name).first()
                        actor_id = actor.id
                        #name = actor.name
                        last_update = actor.last_update.strftime("%Y-%m-%d-%H:%M:%S")
                        return {"id":actor_id,
                                "last_update": last_update,
                                "_links": {
                                    "self": {
                                        "href":"http://127.0.0.1:8888/actors/" + str(actor_id)
                                    }
                                }},201
    
    
@api.route('/actors/<int:id>')
class Actor_get(Resource):
    def get(self, id):
        actor = ActorDB.query.get_or_404(id)
        actor_id = actor.id
        name = actor.name
        birthday = "unknown"
        if actor.birthday:
            birthday = actor.birthday.strftime("%d-%m-%Y")
        country = actor.country
        deathday = actor.deathday
        shows = actor.show
        last_update = actor.last_update.strftime("%Y-%m-%d-%H:%M:%S")
        return_json = {"id":actor_id,
                "last_update": last_update,
                "name":name,
                "country":country,
                "birthday":birthday,
                "deathday":deathday,
                "shows": shows.split(","),
                "_links": {
                    "self": {
                        "href": "http://127.0.0.1:8888/actors/" + str(actor_id)
                    }
                }}
        previous = ActorDB.query.order_by(ActorDB.id.desc()).filter(ActorDB.id < actor_id).first()
        next = ActorDB.query.order_by(ActorDB.id.asc()).filter(ActorDB.id > actor_id).first()
        if previous:
            return_json["_links"]["previous"] = {"href": "http://127.0.0.1:8888/actors/" + str(previous.id)}
        if next:
            return_json["_links"]["next"] = {"href": "http://127.0.0.1:8888/actors/" + str(next.id)}
        return return_json
    
    def delete(self, id):
        actor_delete = ActorDB.query.get_or_404(id)
        db.session.delete(actor_delete)
        db.session.commit()
        return{
            "message":"The actor with id " + str(id) +" was removed from the database!",
            "id": id
        },200

    @api.expect(edit_model)
    def patch(self,id):
        actor_update = ActorDB.query.get_or_404(id)
        update_data = request.get_json()
        update_data = json.loads(json.dumps(update_data))
        if_change = False
        for column, data in update_data.items():
            if column in ["birthday", "deathday"] and data:
                data = datetime.strptime(data,"%d-%m-%Y")
            if getattr(actor_update, column) != data:
                if_change = True
                if data != "string":
                    setattr(actor_update, column, data)
        if(if_change):
            last_update = datetime.now()
            actor_update.last_update = last_update
        db.session.commit()
        last_update = last_update.strftime("%Y-%m-%d-%H:%M:%S")
        return {"id":id,
                        "last_update": last_update,
                        "_links": {
                            "self": {
                                "href":"http://127.0.0.1:8888/actors/" + str(id)
                            }
                        }},200

@api.route('/actors/statistics')
class actor_Stat(Resource):
    @api.expect(actor_stat_rep)
    def get(self):
        args = actor_stat_rep.parse_args()
        format = args["format"]
        group_by = args["by"]
        total = ActorDB.query.count()
        total_update = ActorDB.query.filter(ActorDB.last_update > (datetime.today() - timedelta(days=1))).count()
        return_json = {"total":total,
                    "total_update":total_update}
        fig, axes = plt.subplots(len(group_by),1)
        index = 0
        if "country" in group_by:
            index += 1
            group_by_country = db.session.query(ActorDB.country, db.func.count(ActorDB.id).label("total")).group_by(ActorDB.country).all()
            country_list=[]
            count_list = []
            for data in group_by_country:
                country_list.append(data[0])
                count_list.append(data[1])
            count_list = np.array(count_list)
            count_list = list(count_list / count_list.sum())
            return_json["by-country"] = {country_list[i]: count_list[i] for i in range(len(count_list))}
            labels = country_list
            sizes = count_list
            ax1 = plt.subplot(len(group_by), 1, index)
            ax1.pie(sizes,labels=labels)
            #axes[index].bar(labels, sizes)
        if "birthday" in group_by:
            index +=1
            all_birth = db.session.query(ActorDB.birthday, ActorDB.name).all()
            age_list = []
            name_list = []
            for btd in all_birth:
                if btd.birthday:
                    age =int(datetime.now().strftime("%Y")) - int(btd.birthday.strftime("%Y"))
                    age_list.append(age)
                    name_list.append(btd.name)
            age_list = np.array(age_list)
            return_json["by-birthday"] = {"max-age": int(age_list.max()),
                                            "min-age": int(age_list.min()),
                                            "ave-age":int(age_list.sum()/age_list.size)}
            ax2 = plt.subplot(len(group_by), 1, index)
            ax2.plot(name_list,age_list)
            
        if "gender" in group_by:
            index += 1
            group_by_gender = db.session.query(ActorDB.gender, db.func.count(ActorDB.id).label("total")).group_by(ActorDB.gender).all()
            gender_list=[]
            count_list = []
            for data in group_by_gender:
                if not data[0]:
                    gender_list.append("unknown")
                    count_list.append(data[1])
                else:
                    gender_list.append(data[0])
                    count_list.append(data[1])
            count_list = np.array(count_list)
            count_list = list(count_list / count_list.sum())
            return_json["by-gender"] = {gender_list[i]: count_list[i] for i in range(len(count_list))}
            ax3 = plt.subplot(len(group_by), 1, index)
            ax3.pie(count_list, labels =gender_list, autopct='%1.1f%%',
                    shadow=True, startangle=90)
            #axes[index]=plt.pie(count_list, labels = gender_list, autopct='%1.1f%%',
                    #shadow=True, startangle=90)
        if "life_status" in group_by:
            index+=1
            all_death = db.session.query(ActorDB.deathday).all()
            live = 0
            not_alive = 0
            for d in all_death:
                if d.deathday:
                    not_alive+=1
                else:
                    live+=1
            live_per = live/(live+not_alive)
            not_alive_per = not_alive/(live+not_alive)
            return_json["by-life-status"] = {"alive": live_per,"not-alive":not_alive_per}
            labels = ["alive","not-alive"]
            percent = [live_per,not_alive_per]
            ax4 = plt.subplot(len(group_by), 1, index)
            ax4.pie(percent, labels=labels, autopct='%1.1f%%',
                    shadow=True, startangle=90)
        if format == "json":
            return return_json
        elif format == "image":
            plt.savefig("pie2.png")
            return make_response(send_file("pie2.png"),200)

if __name__ == "__main__":
    db.create_all()
    app.run(host='127.0.0.1', port=8880, debug=True)

