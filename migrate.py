import flask
from flask.views import MethodView
from sqlalchemy import Column, DateTime, Integer, String, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from flask import request, jsonify

app = flask.Flask('app')
PG_DSN = 'postgresql://postgres:postgres@127.0.0.1:5432/async'
engine = create_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(bind=engine)


class StartWars(Base):
    __tablename__ = 'StartWars'
    id = Column(Integer, primary_key=True)
    birth_year = Column(String(100), nullable=False)
    eye_color = Column(String(100), nullable=False)
    films = Column(String(1000), nullable=False)
    gender = Column(String(100), nullable=False)
    hair_color = Column(String(100), nullable=False)
    height = Column(String(100), nullable=False)
    homeworld = Column(String(1000), nullable=False)
    mass = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    skin_color = Column(String(100), nullable=False)
    species = Column(String(1000), nullable=False)
    starships = Column(String(1000), nullable=False)
    vehicles = Column(String(1000), nullable=False)



class StartWarsView(MethodView):

    def get(self,adv_id:int):
        with Session() as session:
            get_data = (
                session.query(StartWars)
                    .filter(
                    StartWars.id == adv_id
                ).first())
        return jsonify({'id':get_data.id,'title':get_data.title ,'description':get_data.description,
                        'create_date':get_data.create_date,'owner':get_data.owner})

    def put(self,adv_id: int):
        new_adv_data = request.json
        with Session() as session:
            session.query(StartWars).filter(StartWars.id == adv_id).update({StartWars.title:new_adv_data['title'],
                                   StartWars.description:new_adv_data['description'],
                                   StartWars.owner:new_adv_data['owner']})
            session.commit()
            get_data = (
                session.query(StartWars)
                    .filter(
                    StartWars.id == adv_id
                ).first())
        return jsonify({'id':get_data.id,'title':get_data.title ,'description':get_data.description,
                        'create_date':get_data.create_date,'owner':get_data.owner})

    def delete(self, adv_id: int):
        with Session() as session:
            session.query(StartWars).filter(StartWars.id == adv_id).delete()
            session.commit()
            return jsonify({'status': 'ok'})

    def post(self):
        new_adv_data = request.json
        with Session() as session:
            new_adv = StartWars(title=new_adv_data['title'],
                               description=new_adv_data['description'],
                               owner=new_adv_data['owner'])
            session.add(new_adv)
            session.commit()
            return flask.jsonify({'id': new_adv.id})


app.add_url_rule('/adv/', view_func=StartWarsView.as_view('create_adv'), methods=['POST'])
app.add_url_rule('/adv/<int:adv_id>', view_func=StartWarsView.as_view('view_adv'), methods=['GET'])
app.add_url_rule('/adv/<int:adv_id>', view_func=StartWarsView.as_view('delete_adv'), methods=['DELETE'])
app.add_url_rule('/adv/<int:adv_id>', view_func=StartWarsView.as_view('edit_adv'), methods=['PUT'])