create or replace PACKAGE BODY          PKG_KPI_CYCLE AS
  
  PROCEDURE P_KPI_REGISTER_HIS_FCYCLE (
    i_input_date date default trunc(sysdate)
  )
    /*
      @Procedure: Ghi nh?n th�ng tin ??ng k� KPI cho [CHU K?];
      @author: thuattq1
      
      @params:  
      i_input_date : Ng�y c?n t?ng h?p th�ng tin ??ng k� (chu?n nh?t l� ng�y cu?i chu k? mu?n ghi nh?n KPI).
    */
  as  
    i_kpi_period KPI_REGISTER.kpi_period%type := 1;-- 1 chu ky; 2: nam;
    
    vv_run_date date;
    vv_kpi_date CYCLE.end_date%type;
    vv_bccycle_date CYCLE.end_date%type; -- ngay dau chu ky hien tai
    vv_eccycle_date CYCLE.end_date%type; -- ngay cuoi chu ky hien tai
    
    vv_period_value KPI_QUOTA.kpi_period_value%type;
    vv_cperiod_value KPI_QUOTA.kpi_period_value%type; -- chu ky hien tai
    vv_is_curent number(1);
  begin
    vv_run_date := sysdate;
    select ce.cycle_id, trunc(ce.begin_date), trunc(ce.end_date)
    into vv_cperiod_value, vv_bccycle_date, vv_eccycle_date
    from CYCLE ce
    where ce.status = 1
      and ce.begin_date < trunc(vv_run_date) + 1
      and ce.end_date >= trunc(vv_run_date);
    
    -- neu: ngay dau chu ky hien tai <= i_input_date < trunc(ngay cuoi chu ky hien tai) + 1: chay cho sysdate
    -- else: chay cho ngay cuoi chu ky i_input_date;
    if (  i_input_date >= vv_bccycle_date
      and i_input_date  < vv_eccycle_date + 1)
    then
      vv_is_curent    := 1;
      vv_kpi_date     := trunc(vv_run_date);
      vv_period_value := vv_cperiod_value;
    else
      vv_is_curent := 0;
      
      select ce.cycle_id, trunc(ce.end_date)
      into vv_period_value, vv_kpi_date
      from CYCLE ce
      where ce.status = 1
        and ce.begin_date < trunc(i_input_date) + 1
        and ce.end_date >= trunc(i_input_date);
    end if;

    MERGE INTO KPI_REGISTER_HIS d
    USING (
      select krr.kpi_register_id, krr.object_type, krr.object_id, krr.kpi_period
        , kgcg.kpi_group_config_id, kgcg.code as group_code, kgcg.name as group_name, kgcg.kpi_group_type
        , kcg.kpi_config_id, kcg.code, kcg.name, kcg.update_type, kcg.order_index, kcg.plan_type
        , kte.procedure_code, kte.code as kpi_type_code, kte.kpi_type_id
        , kgdl.weighted, kgdl.max_value
      from KPI_REGISTER krr
      join KPI_GROUP_CONFIG kgcg 
      on krr.kpi_group_config_id = kgcg.kpi_group_config_id
        and kgcg.status = 1
      join KPI_GROUP_DETAIL kgdl
      on kgcg.kpi_group_config_id = kgdl.kpi_group_config_id
        and ((vv_is_curent = 1 and kgdl.status = 1) 
          or (vv_is_curent = 0 and kgdl.status in (0, 1))) -- qua khu chay ca trang thai 0
        and kgdl.from_kpi_period_value <= vv_period_value
        and (kgdl.to_kpi_period_value  >= vv_period_value 
          or kgdl.to_kpi_period_value is null)
      join KPI_CONFIG kcg
      on kgdl.kpi_config_id =kcg.kpi_config_id
        and kcg.status = 1     
        and kcg.update_type = 2 -- loai t? ??ng
        and kcg.kpi_period = 1 -- chu ky
      join KPI_TYPE kte
      on kte.kpi_type_id = kcg.kpi_type_id 
        and kte.status = 1
        and trim(kte.procedure_code) is not null
      where kgcg.kpi_group_type = 1 -- chu ky
        and ((vv_is_curent = 0 and krr.status in (0, 1))
          or (vv_is_curent = 1 and krr.status = 1))
        and krr.kpi_period = i_kpi_period
          and krr.from_date < trunc(vv_kpi_date) + 1
          and (krr.to_date >= trunc(vv_kpi_date) or krr.to_date is null)
    ) s
    ON (  d.kpi_period        = i_kpi_period
      and d.kpi_period_value  = vv_period_value
      and d.kpi_register_id   = s.kpi_register_id
      and d.object_type       = s.object_type
      and d.object_id         = s.object_id
      and d.kpi_group_config_id = s.kpi_group_config_id
      and d.kpi_config_id     = s.kpi_config_id
      and d.kpi_type_id       = s.kpi_type_id
      and d.procedure_code    = s.procedure_code
    ) 
    WHEN MATCHED THEN
    UPDATE SET
      -- d.kpi_group_config_id = s.kpi_group_config_id,
      d.kpi_group_code = s.group_code,
      d.kpi_group_name = s.group_name,
      d.kpi_group_type = s.kpi_group_type,
      -- d.kpi_config_id = s.kpi_config_id,
      d.kpi_config_code = s.code,
      d.kpi_config_name = s.name,
      d.plan_type = s.plan_type,
      -- d.kpi_type_id = s.kpi_type_id,
      d.kpi_type_code = s.kpi_type_code,
      -- d.procedure_code = s.procedure_code,
      d.weighted = s.weighted,
      d.max_value = s.max_value,
      d.run_date = vv_run_date,
      d.update_date = vv_run_date,
      d.update_user = 'SYS'
    WHEN NOT MATCHED THEN
    INSERT (
      d.kpi_register_his_id, d.kpi_period, d.kpi_period_value,
      d.kpi_register_id, d.object_type, d.object_id,
      d.kpi_group_config_id, d.kpi_group_code, d.kpi_group_name,
      d.kpi_group_type, d.kpi_config_id, d.kpi_config_code,
      d.kpi_config_name, d.plan_type, d.kpi_type_id,
      d.kpi_type_code, d.procedure_code, d.weighted,
      d.max_value, d.run_date, d.create_date,
      d.create_user)
    VALUES (
      seq_nextval_on_demand('KPI_REGISTER_HIS_SEQ'), i_kpi_period, vv_period_value,
      s.kpi_register_id, s.object_type, s.object_id,
      s.kpi_group_config_id, s.group_code, s.group_name,
      s.kpi_group_type, s.kpi_config_id, s.code,
      s.name, s.plan_type, s.kpi_type_id,
      s.kpi_type_code, s.procedure_code, s.weighted,
      s.max_value, vv_run_date, vv_run_date,
      'SYS');
    
    /* X�a d�ng d? li?u t?ng h?p th?a:
       Tr??ng h?p: ko t?n t?i b? ??ng k� h?p l� (KPI_REGISTER, KPI_GROUP_CONFIG, KPI_GROUP_DETAIL, KPI_CONFIG)*/
    delete RPT_KPI_CYCLE rpt
    where (rpt.kpi_register_id, rpt.cycle_id, rpt.kpi_group_config_id, rpt.kpi_config_id) in (
        select d.kpi_register_id
          --, d.kpi_period-- , d.kpi_type_id
          , d.kpi_period_value
          , d.kpi_group_config_id, d.kpi_config_id
        from KPI_REGISTER_HIS d
        where d.kpi_period        = i_kpi_period
          and d.kpi_period_value  = vv_period_value
          and d.run_date          < vv_run_date
      );
      
    /* X�a d�ng d? li?u t?ng h?p th?a:
       Tr??ng h?p: ko khai b�o KPI_QUOTA, ho?c khai b�o KPI_QUOTA.WEIGHTED <= 0*/
    delete RPT_KPI_CYCLE rkce
    where rkce.cycle_id = vv_period_value
      and (
        -- khai bao QUOTA, nhung nvl(weighted, 0) <= 0
        exists (
          select 1
          from KPI_QUOTA kqa
          where rkce.object_id = kqa.object_id
            and ((rkce.object_type = 1 and kqa.object_type  = 2) --NV
              or (rkce.object_type = 3 and kqa.object_type  = 1) --NPP
            )
            and rkce.kpi_register_id = kqa.kpi_register_id
            and rkce.kpi_config_id   = kqa.kpi_config_id
            and kqa.kpi_period_value = rkce.cycle_id
            and not(kqa.weighted is not null and kqa.weighted > 0) -- ko duoc khai bao QUOTA
        )
        -- ko duoc khai bao QUOTA
        or not exists (
          select 1
          from KPI_QUOTA kqa
          where rkce.object_id = kqa.object_id
            and ((rkce.object_type = 1 and kqa.object_type  = 2) --NV
              or (rkce.object_type = 3 and kqa.object_type  = 1) --NPP
            )
            and rkce.kpi_register_id = kqa.kpi_register_id
            and rkce.kpi_config_id   = kqa.kpi_config_id
            and kqa.kpi_period_value = rkce.cycle_id
        )
      );
      
    /*xoa nhung dong tong hop dang ky KPI cho loai NV, 
        nhung ton tai dong dang ky cho NV*/
    delete RPT_KPI_CYCLE rpt
    where rpt.cycle_id = vv_cperiod_value
      and rpt.object_type = 1 -- NV  
      and exists (
        select 1
        from KPI_REGISTER_HIS krr
        where rpt.kpi_register_id = krr.kpi_register_id
          and krr.object_type = 4 -- Loai NV
          -- and krr.kpi_period = 1 and krr.kpi_period_value = rpt.cycle_id
          and krr.run_date >= vv_run_date
      ) -- KPI gan cho loai NV  
      and exists (
        select 1
        from KPI_REGISTER_HIS krr
        where krr.object_id = rpt.object_id 
          and krr.object_type = 2
          and krr.kpi_period = 1 and krr.kpi_period_value = rpt.cycle_id
          and krr.run_date >= vv_run_date
      ) -- nhung ton tai KPI gan cho NV
    ;
    
    -- X�a d�ng ??ng k� th?a.
    delete KPI_REGISTER_HIS d
    where d.kpi_period        = i_kpi_period
      and d.kpi_period_value  = vv_period_value
      and d.run_date          < vv_run_date;
    
    COMMIT;
  end P_KPI_REGISTER_HIS_FCYCLE;
  
  FUNCTION F_CAL_KPI_SCORE (
      i_plan_value number
    , i_gain_value number
    , i_weighted   number
    , i_max_value  number
    , i_param      varchar2
  ) return number
  /*@Procedure: T�nh ?i?m KPI;
    @author: thuattq1
    
    @params:  
      i_plan_value: s? li?u k? ho?ch  
      i_gain_value: s? li?u th?c hi?n
      i_weighted  : tr?ng s?
      i_max_value : c?n tr�n
  */
  is
    vv_gain_per  number;
    vv_core number;
  begin
    if    i_plan_value is null 
      or  i_gain_value is null or i_gain_value = 0
      or (i_plan_value <= 0 and i_gain_value is null)
      or (i_plan_value  > 0 and i_gain_value is not null and i_gain_value <= 0)
    then
      vv_core := 0;
    elsif i_plan_value <= 0 then
      vv_core := round(nvl(i_weighted, 100)/100, 2);
    else 
      -- tinh % hoan thanh
      vv_gain_per := (i_gain_value / i_plan_value) * 100;
      if i_max_value is not null 
          and vv_gain_per > i_max_value 
      then
        vv_gain_per := i_max_value;
      end if;
      
      begin
        with param_tmp as (
          select regexp_substr(i_param,'[^;]+', 1, level) as str
          from dual
          connect by regexp_substr(i_param,'[^;]+', 1, level) is not null
        )
        , scrore_tmp as (
          select 
              to_number(substr(str, 1, instr(str, '-') - 1)) as f
            , to_number(substr(str, instr(str, '-') + 1, instr(str, ':') - instr(str, '-') - 1)) as t
            , to_number(substr(str, instr(str, ':') + 1)) as vl
            --, str , instr(str, '-'), instr(str, ':')
          from param_tmp
        )
        select tmp.vl
        into vv_core
        from scrore_tmp tmp
        where vv_gain_per >= nvl(tmp.f, 0) and vv_gain_per < nvl(tmp.t, 99999) 
        ;
      exception 
      when others then
        vv_core := 0;
      end;
      
      vv_core := round(vv_core * nvl(i_weighted, 100)/100, 2);
    end if;
    
    return vv_core;
  end F_CAL_KPI_SCORE;
  
  FUNCTION F_CAL_KPI_BSCORE (
      i_plan_value  number
    , i_gain_value  number
    , i_param       varchar2
  ) return number
  /*@Procedure: T�nh ?i?m chi ti?t tham s? KPI;
    @author: thuattq1
    
    @params:  
      i_plan_value: s? li?u k? ho?ch  
      i_gain_value: s? li?u th?c hi?n
  */
  is
    vv_gain_per  number;
    vv_core number;
  begin
    if    i_plan_value is null 
      or  i_gain_value is null or i_gain_value = 0
      or (i_plan_value <= 0 and i_gain_value is null)
      or (i_plan_value  > 0 and i_gain_value is not null and i_gain_value <= 0)
    then
      vv_core := 0;
    elsif i_plan_value <= 0 then
      vv_core := 0;
    else 
      -- tinh % hoan thanh
      vv_gain_per := (i_gain_value / i_plan_value) * 100;
      
      begin
        with param_tmp as (
          select regexp_substr(i_param,'[^;]+', 1, level) as str
          from dual
          connect by regexp_substr(i_param,'[^;]+', 1, level) is not null
        )
        , scrore_tmp as (
          select 
              to_number(substr(str, 1, instr(str, '-') - 1)) as f
            , to_number(substr(str, instr(str, '-') + 1, instr(str, ':') - instr(str, '-') - 1)) as t
            , to_number(substr(str, instr(str, ':') + 1)) as vl
            --, str , instr(str, '-'), instr(str, ':')
          from param_tmp
        )
        select tmp.vl
        into vv_core
        from scrore_tmp tmp
        where vv_gain_per >= nvl(tmp.f, 0) and vv_gain_per < nvl(tmp.t, 99999) 
        ;
      exception 
      when others then
        vv_core := 0;
      end;
    end if;
    
    return vv_core;
  end F_CAL_KPI_BSCORE;

  PROCEDURE P_KPI_SALE_PLAN_SHOP_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [Kh?i l??ng ti�u th? v�o h? th?ng so v?i k? ho?ch; C1; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 1: C1 c? th?.
      i_object_id           : ID C1.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_rpt_id        number(20);
    v_error_type    number(2);
    v_error_msg     nvarchar2(2000);
    v_score         RPT_KPI_CYCLE.SCORE%TYPE;
    v_ttscore       RPT_KPI_CYCLE.SCORE%TYPE;
    v_atual_column  varchar2(50);
    v_imp_column    varchar2(50);
    v_is_first      number;
    
    v_param         CONSTANT varchar2(500) := '100-:100;90-100:80;80-90:60;70-80:50;60-70:40;50-60:20;-50:0';
    
    v_pro_name      CONSTANT varchar2(200) := 'P_KPI_SALE_PLAN_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.shop_id%type,
      weighted        KPI_GROUP_DETAIL.weighted%type,
      max_value       KPI_GROUP_DETAIL.max_value%type,
      plan_type       NUMBER(2),
      kpi_product_group_id  KPI_PRODUCT_GROUP.kpi_product_group_id%type,
      sub_weighted    KPI_PARAM_VALUE.weighted%type,
      plan_value      RPT_KPI_CYCLE.plan%type,
      gain            RPT_KPI_CYCLE.done%type
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    if  i_object_type   is null
        or i_object_id  is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1]');
      return;
    end if;
    
    if i_plan_type not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [1]');
      return;
    end if;
    
    if i_kpi_config_code in (
       'AMOUNT_SHOP_GROUP_PRO'
    ) then
    
      v_atual_column:= 'amount';
      v_imp_column  := 'amount_approved';
    elsif i_kpi_config_code in (
        'QUANTITY_SHOP_GROUP_PRO'
    ) then
    
      v_atual_column:= 'quantity';
      v_imp_column  := 'quantity_approved';
    else
    
      insert_log_procedure(v_pro_name, NULL, NULL, 3, i_kpi_config_code || '- invalidate kpi config code');
      return;
    end if;
    
    v_sql := 
      -- dsach C1 tinh KPI
     'with pt_tmp as (
        select kpgp.kpi_product_group_id, pvl.weighted
          , pt.product_id, nullif(pt.convfact, 0) as convfact
        from (
            select distinct kpve.value, kpve.weighted
            from KPI_PARAM_VALUE kpve
            join KPI_PARAM kpm 
            on kpm.kpi_param_id = kpve.kpi_param_id 
              and kpm.status = 1 and kpm.type = 13
            where kpve.kpi_config_id = ' || i_kpi_config_id || '
              and kpve.status in (0, 1)
              and kpve.from_kpi_period_value <= ' || i_kpi_period_value || '
              and (kpve.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpve.to_kpi_period_value is null)
        ) pvl
        join KPI_PRODUCT_GROUP kpgp
        on kpgp.code = pvl.value
        join KPI_PRODUCT_GROUP_DTL kpgdl
        on kpgdl.kpi_product_group_id = kpgp.kpi_product_group_id
          and kpgdl.status = 1
          and kpgdl.from_kpi_period_value <= ' || i_kpi_period_value || '
          and (kpgdl.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpgdl.to_kpi_period_value is null)
        join PRODUCT pt 
        on kpgdl.product_id = pt.product_id
          and pt.status = 1
        where kpgp.status = 1
      )
      , sp_tmp as (
        select sp.shop_id
        from SHOP sp
        where sp.status = 1 
          and sp.shop_id = ' || i_object_id || ' 
      )
      , splan_tmp as (
        select spp.shop_id
            --, pt.product_id, nullif(pt.convfact, 0) as convfact
            , pt.kpi_product_group_id, pt.weighted
            --, sum(nvl(spn.'||v_atual_column||', 0)) as plan
            , sum(nvl(case  when spn.unit = 1 then (spn.'||v_atual_column||' * pt.convfact) 
                            when spn.unit = 0 then spn.'||v_atual_column||' 
                            else 0 end
              , 0)) as plan
        from sp_tmp spp
        join SALE_PLAN spn
        on spp.shop_id = spn.object_id
        join pt_tmp pt
          on spn.product_id = pt.product_id
        where spn.cycle_id = ' || i_kpi_period_value || '
            and spn.'||v_atual_column||' is not null 
            and spn.object_type = 3
            and spn.type = 3
            and spn.status = 1 
        group by spp.shop_id
          --, pt.product_id, pt.convfact
          , pt.kpi_product_group_id, pt.weighted
      )
      , rpt_tmp as (
        select spp.shop_id
            --, pt.product_id, nullif(pt.convfact, 0) as convfact 
            , pt.kpi_product_group_id, pt.weighted
            , sum(nvl(rptt.' || v_imp_column || ', 0)) gain
        from sp_tmp spp
        join RPT_SALE_PRIMARY_MONTH rptt
        on spp.shop_id = rptt.shop_id
        join pt_tmp pt
        on rptt.product_id = pt.product_id
        where rptt.cycle_id = ' || i_kpi_period_value || ' 
        group by spp.shop_id
          --, pt.product_id, pt.convfact
          , pt.kpi_product_group_id, pt.weighted
      )
      , dta_tmp as (
        select splan_tmp.shop_id, splan_tmp.kpi_product_group_id, splan_tmp.weighted
          , splan_tmp.plan, rpt_tmp.gain
        from splan_tmp
        left join rpt_tmp
        on    splan_tmp.shop_id = rpt_tmp.shop_id
          and splan_tmp.kpi_product_group_id = rpt_tmp.kpi_product_group_id
      )
      select sp.shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , rpt.kpi_product_group_id, rpt.weighted as sub_weighted
          , rpt.plan as plan_value, rpt.gain
      from KPI_REGISTER_HIS vkrr
      join sp_tmp sp 
      on sp.shop_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
      join (
         select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
         from KPI_QUOTA kqat
         where kqat.kpi_period_value = ' || i_kpi_period_value || '
             and kqat.object_type = 1
             and kqat.status = 1
             and kqat.weighted is not null
             and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
             and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sp.shop_id 
      left join dta_tmp rpt
      on sp.shop_id = rpt.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type
    ;

    dbms_output.put_line(v_sql);
    
    -- check dong dau tien
    v_is_first := 0;
    -- tinh diem cho KPI
    v_ttscore := 0;
        
    OPEN c_dta FOR v_sql;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;
        
        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          if v_is_first = 0 then
            begin
              select rpt_kpi_cycle_id
              into v_rpt_id
              from RPT_KPI_CYCLE rpt
              where rpt.cycle_id = i_kpi_period_value
                --and rpt.shop_id = v_dta(indx).shop_id
                and rpt.kpi_group_config_id = i_kpi_group_config_id
                and kpi_config_id = i_kpi_config_id
                and kpi_register_id = i_kpi_reg_id
                and rpt.object_type = 3 -- C1
                and rpt.object_id = v_dta(indx).shop_id;
                  
              v_error_type := 0;
            exception
            when no_data_found then
              v_error_type := 1;
            when too_many_rows then
              v_error_type := 2;
            when others then
              v_error_type := 3;
              v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
            end;
            
            if v_error_type = 0 then
              -- xoa dong detail
              delete RPT_KPI_CYCLE_DTL
              where rpt_kpi_cycle_id = v_rpt_id;
            elsif v_error_type = 1 
              and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
            then
              v_rpt_id := rpt_kpi_cycle_seq.nextval;
              
              insert into RPT_KPI_CYCLE (
                 rpt_kpi_cycle_id, cycle_id, shop_id, 
                 kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
                 weighted,
                 max_value, create_date, create_user) 
              values ( v_rpt_id,
               i_kpi_period_value,
               v_dta(indx).shop_id,
               i_kpi_group_config_id,
               3,
               v_dta(indx).shop_id,
               i_kpi_config_id,
               i_kpi_reg_id,
               v_dta(indx).weighted, 
               i_max_value,
               sysdate,
               'SYS');
            elsif v_error_type = 2 then
              -- xoa dong detail
              delete RPT_KPI_CYCLE_DTL
              where rpt_kpi_cycle_id = v_rpt_id;
              
              delete RPT_KPI_CYCLE
              where rpt_kpi_cycle_id = v_rpt_id;
              
              v_rpt_id := rpt_kpi_cycle_seq.nextval;
              
              insert into RPT_KPI_CYCLE (
                 rpt_kpi_cycle_id, cycle_id, shop_id, 
                 kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
                 weighted,
                 max_value, create_date, create_user) 
              values ( v_rpt_id,
               i_kpi_period_value,
               v_dta(indx).shop_id,
               i_kpi_group_config_id,
               3,
               v_dta(indx).shop_id,
               i_kpi_config_id,
               i_kpi_reg_id,
               v_dta(indx).weighted, 
               i_max_value, 
               sysdate,
               PKG_KPI_CYCLE.g_imp_user);
            elsif v_error_type = 3 then
              insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;kpi_group_config_id;kpi_config_id]=['
                ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
                ||v_error_msg);
            end if;
              
            v_is_first := 1;
          end if;
          
          v_score := PKG_KPI_CYCLE.F_CAL_KPI_BSCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_param
          );
          
          v_ttscore := v_ttscore + nvl(round(v_score * v_dta(indx).sub_weighted / 100, 2), 0);
          
          INSERT INTO RPT_KPI_CYCLE_DTL (
             RPT_KPI_CYCLE_DTL_ID, RPT_KPI_CYCLE_ID, CYCLE_ID, 
             KPI_PARAM_VALUE_ID, PLAN, DONE, 
             SCORE_B, SCORE, WEIGHTED, 
             MAX_VALUE, CREATE_DATE, CREATE_USER) 
          VALUES ( 
            RPT_KPI_CYCLE_DTL_SEQ.nextval,
            v_rpt_id,
            i_kpi_period_value,
            v_dta(indx).kpi_product_group_id,
            v_dta(indx).plan_value,
            v_dta(indx).gain,
            v_score,
            nvl(round(v_score * v_dta(indx).sub_weighted / 100, 2), 0),
            v_dta(indx).sub_weighted,
            null,
            sysdate,
            PKG_KPI_CYCLE.g_imp_user);
          
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    dbms_output.put_line('total score' || v_ttscore);
    update RPT_KPI_CYCLE
    set   score            = nvl(round(v_ttscore * weighted / 100, 2), 0),
          max_value        = i_max_value,
          update_date      = sysdate,
          update_user      = PKG_KPI_CYCLE.g_imp_user
    where rpt_kpi_cycle_id = v_rpt_id;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  /*exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));*/
  end P_KPI_SALE_PLAN_SHOP_CYCLE;
  
  PROCEDURE P_KPI_PO_RECEIVE_SHOP_CYCLE (
    i_object_type number,
    i_object_id number,
    i_kpi_period_value number,
    i_kpi_group_config_id number,
    i_kpi_config_id number,
    i_kpi_config_code varchar2,
    i_plan_type number,
    i_kpi_reg_id number,
    i_max_value number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [doanh s?, s?n l??ng nh?p; npp; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 1: NPP c? th?.
    i_object_id           : ID NPP.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_group_column varchar2(100);
    v_rpt_id number(20);
    v_error_type number(2);
    v_error_msg nvarchar2(2000);
    v_params nvarchar2(2000);
    v_score RPT_KPI_CYCLE.SCORE%TYPE;
    v_weighted number;
    --v_atual_column  varchar2(50);
    v_count_param   number;
    v_cycle_edate CYCLE.end_date%type;
    
    v_param         CONSTANT varchar2(500) := '100-:100;90-100:80;80-90:60;70-80:50;60-70:40;50-60:20;-50:0';
    
    v_pro_name      constant varchar2(200) := 'P_KPI_PO_RECEIVE_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    

    if  i_object_type is null
      or i_object_id is null
      or i_kpi_period_value is null
      or i_kpi_group_config_id is null
      or i_kpi_config_id is null
      or i_kpi_config_code is null
      or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1]');
      return;
    end if;
    
    begin
      select trunc(ce.end_date)
      into v_cycle_edate
      from CYCLE ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;
        
    if i_kpi_config_code in ('PO_RECEIVE_SHOP_PRODUCT_GROUP') then 
        -- v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm 
        on kpm.kpi_param_id = kpve.kpi_param_id 
          and kpm.status = 1 and kpm.type = 13
        where kpve.kpi_config_id =  i_kpi_config_id 
          and kpve.status in (0, 1)
          and kpve.from_kpi_period_value <= i_kpi_period_value 
          and (kpve.to_kpi_period_value >= i_kpi_period_value or kpve.to_kpi_period_value is null)
        ;
        
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select kpgdl.product_id
              from KPI_PRODUCT_GROUP kpgp
              join KPI_PRODUCT_GROUP_DTL kpgdl
              on kpgdl.kpi_product_group_id = kpgp.kpi_product_group_id
                and kpgdl.status = 1
                and kpgdl.from_kpi_period_value <= ' || i_kpi_period_value || '
                and (kpgdl.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpgdl.to_kpi_period_value is null)
              where kpgp.status = 1
                and exists (
                  select 1
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm 
                  on kpm.kpi_param_id = kpve.kpi_param_id 
                    and kpm.status = 1 and kpm.type = 13
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpve.from_kpi_period_value <= ' || i_kpi_period_value || '
                    and (kpve.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpve.to_kpi_period_value is null)
                    and kpgp.code = pvl.value
                )
            ) ';
        end if;
    elsif i_kpi_config_code in ('PO_RECEIVE_SHOP_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
     'select sp.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          '||(case when i_plan_type = 2 then ', kqa.plan_value ' 
                   when i_plan_type = 1 then ', spn.plan ' 
                   else ', null ' end)||' as plan_value
          , rpt.gain
      from KPI_REGISTER_HIS vkrr
      join SHOP sp 
      on sp.status = 1 
        and sp.shop_id = vkrr.object_id and sp.shop_id = ' || i_object_id ||
    (case when i_plan_type = 1 then '
      join (
        select po.shop_id --, pt.product_id, pdl.price
          , sum(nvl(pdl.quantity, 0)) as plan
        from PO
        join PO_DETAIL pdl
        on po.po_id = pdl.po_id
        join PRODUCT pt
        on pdl.product_id = pt.product_id
          and pt.status = 1
        where po.status = 1
          and po.po_type = 1
          and po.to_payment_date >= to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'')
          and po.to_payment_date <  to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and po.shop_id = ' || i_object_id || '
          '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
        group by po.shop_id
      ) spn 
      on spn.object_id = sp.shop_id 
      '
     end) ||    
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 1
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa 
      on kqa.object_id = sp.shop_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        select pvm.shop_id
          --, pt.product_id, pvdl.price
          , sum(nvl(pvdl.quantity, 0)) as gain
        from PO_VNM pvm
        join PO_VNM_DETAIL pvdl
        on pvm.po_vnm_id = pvdl.po_vnm_id
        join PRODUCT pt
        on pt.product_id = pvdl.product_id
          and pt.status = 1
        where pvm.type = 2
          and pvm.po_vnm_date >= to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'') 
          and pvm.po_vnm_date <  to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and pvm.shop_id = ' || i_object_id || '
          '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
        group by pvm.shop_id
      ) rpt
      on sp.shop_id = rpt.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 3 -- npp
              and rpt.object_id = v_dta(indx).shop_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_CYCLE.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
            , v_param
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).shop_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  /*exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));*/
  end P_KPI_PO_RECEIVE_SHOP_CYCLE;
  
  PROCEDURE P_KPI_AMOUNT_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date  date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [doanh s?, s?n l??ng; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
      i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
      i_object_id           : ID nh�n vi�n/lo?i nv.
      i_kpi_period_value    : ID gi� tr? k?.
      i_kpi_group_config_id : ID nh�m KPI.
      i_kpi_config_id       : ID KPI.
      i_kpi_config_code     : M� KPI.
      i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?; 2: c� ph�n b?.
      i_kpi_reg_id          : ID KPI_REGISTER.
      i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    -- v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.score%TYPE;
    v_ttscore RPT_KPI_CYCLE.score%TYPE;
    v_atual_column  varchar2(50);
    v_imp_column    varchar2(50);
    v_count_param   number;
    vv_specific_type  STAFF_TYPE.specific_type%TYPE;
    vv_pre_staff_id   STAFF.staff_id%type;
    
    v_param         CONSTANT varchar2(500) := '100-:100;90-100:80;80-90:60;70-80:50;60-70:40;50-60:20;-50:0';
    
    v_pro_name      constant varchar2(200) := 'P_KPI_AMOUNT_STAFF_CYCLE';
    v_p_start_time  date;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.staff_id%type,
      shop_id         SHOP.shop_id%type,
      weighted        KPI_GROUP_DETAIL.weighted%type,
      max_value       KPI_GROUP_DETAIL.max_value%type,
      plan_type       NUMBER(2),
      kpi_product_group_id  KPI_PRODUCT_GROUP.kpi_product_group_id%type,
      sub_weighted    KPI_PARAM_VALUE.weighted%type,
      plan_value      RPT_KPI_CYCLE.plan%type,
      gain            RPT_KPI_CYCLE.done%type,
      gain_ir         RPT_KPI_CYCLE.done_ir%type,
      gain_or         RPT_KPI_CYCLE.done_or%type
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type   is null
        or i_object_id  is null
        or v_kpi_period is null
        or i_kpi_period_value     is null
        or i_kpi_group_config_id  is null
        or i_kpi_config_id    is null
        or i_kpi_config_code  is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE ste
      on sf.staff_type_id = ste.staff_type_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [8: NVTT]
    if vv_specific_type not in (8) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [8]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    if i_kpi_config_code in (
         /*'AMOUNT_STAFF_PRODUCT' , 'AMOUNT_STAFF_CAT'    , 'AMOUNT_STAFF_SUBCAT'
       , 'AMOUNT_STAFF_BRAND'   , 'AMOUNT_STAFF_FLAVOUR', 'AMOUNT_STAFF_PACKING'
       , 'AMOUNT_STAFF_UOM'     , 'AMOUNT_STAFF_VOLUMN', 'AMOUNT_STAFF_ALL'*/
       'AMOUNT_STAFF_GROUP_PRO'
    ) then
    
      v_atual_column:= 'amount';
      v_imp_column  := 'amount_approved';
    elsif i_kpi_config_code in (
         /*'QUANTITY_STAFF_PRODUCT' , 'QUANTITY_STAFF_CAT'    , 'QUANTITY_STAFF_SUBCAT'
       , 'QUANTITY_STAFF_BRAND'   , 'QUANTITY_STAFF_FLAVOUR', 'QUANTITY_STAFF_PACKING'
       , 'QUANTITY_STAFF_UOM'     , 'QUANTITY_STAFF_VOLUMN', 'QUANTITY_STAFF_ALL' */
       'QUANTITY_STAFF_GROUP_PRO'
    ) then
    
      v_atual_column:= 'quantity';
      v_imp_column  := 'quantity_approved';
    else
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'invalidate kpi config code');
      return;
    end if;
    
    if vv_specific_type = 8 then 
      -- mac dinh NV vv_specific_type = 1
      v_sql := 
        -- ds NV tinh KPI
       'with pt_tmp as (
          select kpgp.kpi_product_group_id, pvl.weighted
            , pt.product_id, nullif(pt.convfact, 0) as convfact
          from (
              select distinct kpve.value, kpve.weighted
              from KPI_PARAM_VALUE kpve
              join KPI_PARAM kpm 
              on kpm.kpi_param_id = kpve.kpi_param_id 
                and kpm.status = 1 and kpm.type = 13
              where kpve.kpi_config_id = ' || i_kpi_config_id || '
                and kpve.status in (0, 1)
                and kpve.from_kpi_period_value <= ' || i_kpi_period_value || '
                and (kpve.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpve.to_kpi_period_value is null)
          ) pvl
          join KPI_PRODUCT_GROUP kpgp
          on kpgp.code = pvl.value
          join KPI_PRODUCT_GROUP_DTL kpgdl
          on kpgdl.kpi_product_group_id = kpgp.kpi_product_group_id
            and kpgdl.status = 1
            and kpgdl.from_kpi_period_value <= ' || i_kpi_period_value || '
            and (kpgdl.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpgdl.to_kpi_period_value is null)
          join PRODUCT pt 
          on kpgdl.product_id = pt.product_id
            and pt.status = 1
          where kpgp.status = 1
        )
        , sf_tmp as (
          select distinct sf.staff_id, sf.shop_id
            , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
          from STAFF sf 
          join STAFF_TYPE ste 
          on ste.staff_type_id = sf.staff_type_id
          where sf.status = 1 ' ||
            (case when i_object_type = 2 then ' and sf.staff_id = ' || i_object_id || ' '
                  when i_object_type = 4 then ' and ste.staff_type_id = ' || i_object_id || ' '
             end) || ' 
        )
        , splan_tmp as (
          select sff.staff_id
              , pt.kpi_product_group_id, pt.weighted 
              -- , sum(nvl(spn.'||v_atual_column||', 0)) as plan
              , sum(nvl(case  when spn.unit = 1 then (spn.'||v_atual_column||' * pt.convfact) 
                              when spn.unit = 0 then spn.'||v_atual_column||' 
                              else 0 end
                , 0)) as plan
          from sf_tmp sff
          join SALE_PLAN spn
          on sff.staff_id = spn.object_id
          join pt_tmp pt
            on spn.product_id = pt.product_id
          where spn.cycle_id = ' || i_kpi_period_value || '
              and spn.'||v_atual_column||' is not null 
              and spn.object_type = 1
              and spn.type = 2
              and spn.status = 1
          group by sff.staff_id
            , pt.kpi_product_group_id, pt.weighted
        )
        , rpt_tmp as (
          select sff.staff_id
            , pt.kpi_product_group_id, pt.weighted
            , sum(nvl(rptt.' || v_imp_column || ', 0)) gain
            , sum(case when rptt.route_type = 1 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_ir
            , sum(case when rptt.route_type = 0 then nvl(rptt.' || v_imp_column || ', 0) else 0 end) gain_or
          from sf_tmp sff
          join RPT_SALE_PRIMARY_MONTH rptt
          on sff.staff_id = rptt.staff_id
          join pt_tmp pt
          on rptt.product_id = pt.product_id
          where rptt.cycle_id = ' || i_kpi_period_value || ' 
          group by sff.staff_id
            , pt.kpi_product_group_id, pt.weighted
        )
        select sf.staff_id, sf.shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
            , vkrr.plan_type, spn.kpi_product_group_id, spn.weighted as sub_weighted
            , sum(nvl(spn.plan, 0)) as plan_value
            , sum(nvl(rpt.gain, 0)) as gain
            , sum(nvl(rpt.gain_ir, 0)) as gain_ir
            , sum(nvl(rpt.gain_or, 0)) as gain_or
        from KPI_REGISTER_HIS vkrr
        join sf_tmp sf 
        on 1 = 1 ' ||
          (case when i_object_type = 2 then 
                  ' and sf.staff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
                when i_object_type = 4 then 
                  ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                    and sf.staff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
           end) ||' 
        join (
           select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
           from KPI_QUOTA kqat
           where kqat.kpi_period_value = ' || i_kpi_period_value || '
               and kqat.object_type = 2
               and kqat.status = 1
               and kqat.weighted is not null
               and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
               and kqat.kpi_config_id = ' || i_kpi_config_id || '
        ) kqa on kqa.object_id = sf.staff_id
        join splan_tmp spn 
        on spn.staff_id = sf.staff_id 
        left join rpt_tmp rpt
        on sf.staff_id = rpt.staff_id
          and spn.kpi_product_group_id = rpt.kpi_product_group_id
        where vkrr.kpi_period = 1 -- chu ky
          and vkrr.kpi_period_value = ' || i_kpi_period_value || '
          and vkrr.object_type = ' || i_object_type || '
          and vkrr.kpi_config_id = ' || i_kpi_config_id || '
          and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
          and vkrr.plan_type = ' || i_plan_type || '
        group by sf.staff_id, sf.shop_id, kqa.weighted, vkrr.weighted, vkrr.max_value
            , vkrr.plan_type, spn.kpi_product_group_id, spn.weighted
        order by sf.staff_id' -- sap xep de group lai 
      ;
    end if;

    dbms_output.put_line(v_sql);
    
    -- check dong NV
    vv_pre_staff_id := null;
        
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          if vv_pre_staff_id is null 
              or nvl(vv_pre_staff_id, -1) != nvl(v_dta(indx).staff_id, -1) 
          then
            -- cho cho NV truoc
            if vv_pre_staff_id is not null then
            
              update RPT_KPI_CYCLE
              set   score            = nvl(round(v_ttscore * weighted / 100, 2), 0),
                    max_value        = i_max_value,
                    update_date      = sysdate,
                    update_user      = PKG_KPI_CYCLE.g_imp_user
              where rpt_kpi_cycle_id = v_rpt_id;
            end if;
            
            vv_pre_staff_id := v_dta(indx).staff_id;
            v_ttscore := 0;
            
            begin
              select rpt_kpi_cycle_id
              into v_rpt_id
              from RPT_KPI_CYCLE rpt
              where rpt.cycle_id = i_kpi_period_value
                and rpt.shop_id = v_dta(indx).shop_id
                and rpt.kpi_group_config_id = i_kpi_group_config_id
                and kpi_config_id = i_kpi_config_id
                and kpi_register_id = i_kpi_reg_id
                and rpt.object_type = 1 -- nhan vien
                and rpt.object_id = v_dta(indx).staff_id
              ;
                  
              v_error_type := 0;
            exception
            when no_data_found then
              v_error_type := 1;
            when too_many_rows then
              v_error_type := 2;
            when others then
              v_error_type := 3;
              v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
            end;
            
            if v_error_type = 0 then
              -- xoa dong detail
              delete RPT_KPI_CYCLE_DTL
              where rpt_kpi_cycle_id = v_rpt_id;
            elsif v_error_type = 1 
              and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
            then
              v_rpt_id := rpt_kpi_cycle_seq.nextval;
              
              insert into RPT_KPI_CYCLE (
                 rpt_kpi_cycle_id, cycle_id, shop_id, 
                 kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
                 weighted,
                 max_value, create_date, create_user) 
              values ( v_rpt_id,
               i_kpi_period_value,
               v_dta(indx).shop_Id,
               i_kpi_group_config_id,
               1,
               v_dta(indx).staff_id,
               i_kpi_config_id,
               i_kpi_reg_id,
               v_dta(indx).weighted, 
               i_max_value,
               sysdate,
               'SYS');
            elsif v_error_type = 2 then
              -- xoa dong detail
              delete RPT_KPI_CYCLE_DTL
              where rpt_kpi_cycle_id = v_rpt_id;
              
              delete RPT_KPI_CYCLE
              where  rpt_kpi_cycle_id = v_rpt_id;
              
              v_rpt_id := rpt_kpi_cycle_seq.nextval;
              
              insert into RPT_KPI_CYCLE (
                 rpt_kpi_cycle_id, cycle_id, shop_id, 
                 kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
                 weighted,
                 max_value, create_date, create_user) 
              values ( v_rpt_id,
               i_kpi_period_value,
               v_dta(indx).shop_Id,
               i_kpi_group_config_id,
               1,
               v_dta(indx).staff_id,
               i_kpi_config_id,
               i_kpi_reg_id,
               v_dta(indx).weighted, 
               i_max_value, 
               sysdate,
               'SYS');
            elsif v_error_type = 3 then
              insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
                ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
                ||v_error_msg);
            end if;
          end if;
          
          v_score := PKG_KPI_CYCLE.F_CAL_KPI_BSCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_param
          );
          
          v_ttscore := v_ttscore + nvl(round(v_score * v_dta(indx).sub_weighted / 100, 2), 0);
          
          INSERT INTO RPT_KPI_CYCLE_DTL (
             RPT_KPI_CYCLE_DTL_ID, RPT_KPI_CYCLE_ID, CYCLE_ID, 
             KPI_PARAM_VALUE_ID, PLAN, DONE, 
             SCORE_B, SCORE, WEIGHTED, 
             MAX_VALUE, CREATE_DATE, CREATE_USER) 
          VALUES ( 
            RPT_KPI_CYCLE_DTL_SEQ.nextval,
            v_rpt_id,
            i_kpi_period_value,
            v_dta(indx).kpi_product_group_id,
            v_dta(indx).plan_value,
            v_dta(indx).gain,
            v_score,
            nvl(round(v_score * v_dta(indx).sub_weighted / 100, 2), 0),
            v_dta(indx).sub_weighted,
            null,
            sysdate,
            PKG_KPI_CYCLE.g_imp_user);
          
          
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    
    -- cho cho NV truoc
    if vv_pre_staff_id is not null then
    
      update RPT_KPI_CYCLE
      set   score            = nvl(round(v_ttscore * weighted / 100, 2), 0),
            max_value        = i_max_value,
            update_date      = sysdate,
            update_user      = PKG_KPI_CYCLE.g_imp_user
      where rpt_kpi_cycle_id = v_rpt_id;
    end if;
            
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  /*exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));*/
  end P_KPI_AMOUNT_STAFF_CYCLE;
  
  PROCEDURE P_KPI_CUS_VISIT_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [k? ho?ch l? tr�nh; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.score%TYPE;
    v_weighted    number;
    v_count_param number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    vv_bcycle_date  CYCLE.begin_date%type;
    vv_ecycle_date  CYCLE.end_date%type;
    
    v_param         CONSTANT varchar2(500) := '90-:100;50-90:50;-50:0';
    
    v_pro_name      constant varchar2(200) := 'P_KPI_CUS_VISIT_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type   is null
        or i_object_id  is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    begin
      select trunc(begin_date), trunc(end_date)
      into vv_bcycle_date, vv_ecycle_date
      from cycle ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;    
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE ste
      on sf.staff_type_id = ste.staff_type_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVTT;]
    if vv_specific_type not in (8) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [8]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    v_sql := 
      -- ds NV tinh KPI
     'with sf_tmp as (
        select distinct sf.staff_id, sf.shop_id
          , ste.specific_type as ispecific_type, ste.staff_type_id as istaff_type_id
        from STAFF sf 
        join STAFF_TYPE ste 
        on ste.staff_type_id = sf.staff_type_id
        where sf.status = 1 ' ||
         (case when i_object_type = 2 then 
                  ' and sf.staff_id = ' || i_object_id || ' '
                when i_object_type = 4 then 
                  ' and ste.staff_type_id = ' || i_object_id || ' '
          end)|| ' 
      )
      select sf.staff_id as staff_id, sf.shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round(nvl(rpt.gain, 0) * 100/ nullif(spn.plan, 0), 2) as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.staff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.staff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.staff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      join (
        select sff.staff_id, count(distinct cr.customer_id) plan
        from sf_tmp sff
        join VISIT_PLAN vpn
        on vpn.staff_id = sff.staff_id
        join ROUTING rg
        on rg.status = 1
          and rg.shop_id = vpn.shop_id
          and vpn.routing_id = rg.routing_id
        join ROUTING_CUSTOMER rcr
        on rcr.status = 1 
          and rcr.routing_id = rg.routing_id
        join CUSTOMER cr
        on cr.status = 1
          and rcr.customer_id = cr.customer_id
        where vpn.status = 1
          and exists (
            select 1 
            from CYCLE ce
            where ce.cycle_id = ' || i_kpi_period_value || '
              and vpn.from_date <= ce.end_date
              and (vpn.to_date >= ce.begin_date or vpn.to_date is null)
              and rcr.start_date <= ce.end_date
              and (rcr.end_date >= ce.begin_date or rcr.end_date is null)
          )
        group by sff.staff_id
      ) spn 
      on spn.staff_id = sf.staff_id
      left join (
        select sff.staff_id, count(distinct alg.customer_id) as gain
        from sf_tmp sff
        join ACTION_LOG alg
        on sff.staff_id = alg.staff_id
        where alg.object_type in (0, 1)
          and alg.start_time >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and alg.start_time  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and exists (
            select 1
            from CUSTOMER cr
            where cr.customer_id = alg.customer_id
              and cr.status = 1)
        group by sff.staff_id
      ) rpt
      on sf.staff_id = rpt.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_CYCLE.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
            , v_param
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  /*exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));*/
  end P_KPI_CUS_VISIT_STAFF_CYCLE;
  
  PROCEDURE P_KPI_REPORT_STAFF_CYCLE (
    i_object_type number,
    i_object_id   number,
    i_kpi_period_value    number,
    i_kpi_group_config_id number,
    i_kpi_config_id   number,
    i_kpi_config_code varchar2,
    i_plan_type   number,
    i_kpi_reg_id  number,
    i_max_value   number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [k? ho?ch l? tr�nh; nh�n vi�n; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 2: nh�n vi�n; 4: lo?i nh�n vi�n.
    i_object_id           : ID nh�n vi�n/lo?i nv.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 2: c� ph�n b? (ch? l?y ph�n b?).
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_kpi_period    number;
    v_group_column  varchar2(100);
    v_rpt_id      number(20);
    v_error_type  number(2);
    v_error_msg   nvarchar2(2000);
    v_params  nvarchar2(2000);
    v_score   RPT_KPI_CYCLE.score%TYPE;
    v_weighted    number;
    v_count_param number;
    vv_specific_type STAFF_TYPE.specific_type%TYPE;
    vv_bcycle_date  CYCLE.begin_date%type;
    vv_ecycle_date  CYCLE.end_date%type;
    
    v_param         CONSTANT varchar2(500) := '90-:100;50-90:50;-50:0';
    
    v_pro_name      constant varchar2(200) := 'P_KPI_REPORT_STAFF_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      staff_id        STAFF.STAFF_ID%TYPE,
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    
    v_kpi_period := 1;

    if  i_object_type   is null
        or i_object_id  is null
        or v_kpi_period is null
        or i_kpi_period_value is null
        or i_kpi_group_config_id is null
        or i_kpi_config_id is null
        or i_kpi_config_code is null
        or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_object_type not in (2, 4) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [2; 4]');
      return;
    end if;
    
    begin
      select trunc(begin_date), trunc(end_date)
      into vv_bcycle_date, vv_ecycle_date
      from cycle ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;    
    
    if i_object_type = 2 then
      select ste.specific_type
      into vv_specific_type
      from STAFF sf
      join STAFF_TYPE ste
      on sf.staff_type_id = ste.staff_type_id
      where sf.status = 1
        and sf.staff_id = i_object_id;
    elsif i_object_type = 4 then
      select ste.specific_type
      into vv_specific_type
      from STAFF_TYPE ste
      where ste.status = 1
        and ste.staff_type_id = i_object_id;
    end if;
    
    -- check loai NV: [1: NVTT;]
    if vv_specific_type not in (8) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'vv_specific_type not in [8]');
      return;
    end if;
    
    if i_plan_type not in (2) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [2]');
      return;
    end if;
    
    if v_kpi_period not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'v_kpi_period not in [1]');
      return;
    end if;
    
    v_sql := 
      -- ds NV tinh KPI
     'with sf_tmp as (
        select distinct sf.staff_id, sf.shop_id
          , ste.specific_type, ste.staff_type_id
          , f_workings_days_cumulate(to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
              , to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd''), '' '', sf.shop_id, null) as wkday
        from STAFF sf 
        join STAFF_TYPE ste 
        on ste.staff_type_id = sf.staff_type_id
        where sf.status = 1 ' ||
         (case when i_object_type = 2 then 
                  ' and sf.staff_id = ' || i_object_id || ' '
                when i_object_type = 4 then 
                  ' and ste.staff_type_id = ' || i_object_id || ' '
          end)|| ' 
      )
      , day_tmp as (
        select (to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + (level - 1)) as w_day
        from dual
        where to_char(to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + (level - 1), ''d'') = 6
        connect by to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + (level - 1) < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
      )
      , plan_tmp as (
        select sf_tmp.staff_id, sf_tmp.shop_id
            , sf_tmp.specific_type, sf_tmp.staff_type_id
            , (nvl(sf_tmp.wkday, 0) + count(distinct day_tmp.w_day) + 1) as tt_wkday
            -- ngay lam viec + ngay thu 6() + 1 (bc thang)
        from sf_tmp
        left join day_tmp
        on day_tmp.w_day not in (
            select trunc(edy.day_off)
            from EXCEPTION_DAY edy 
            where edy.shop_id = sf_tmp.shop_id 
          )
        group by sf_tmp.staff_id, sf_tmp.shop_id
            , sf_tmp.specific_type, sf_tmp.staff_type_id
            , sf_tmp.wkday
      )
      , rpt_day as (
        select sf_tmp.staff_id, count(distinct trunc(rpt.check_date)) as tt_day 
        from sf_tmp
        join PRICE_CHECK_DATE rpt
        on sf_tmp.staff_id = rpt.staff_id
        where rpt.check_date >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and rpt.check_date  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and trunc(rpt.check_date) = trunc(rpt.create_date) -- bao cao dung ngay  
        group by sf_tmp.staff_id
      )
      , rpt_week as (
        select sf_tmp.staff_id, count(distinct trunc(rpt.check_month)) as tt_day 
        from sf_tmp
        join PRICE_CHECK_MONTH rpt
        on sf_tmp.staff_id = rpt.staff_id
        where rpt.check_week is not null -- b/c tuan
          and rpt.check_month >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and rpt.check_month  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and FIND_NEXT_DAY(trunc(rpt.check_month), 6, rpt.check_week) = trunc(rpt.create_date) -- bao cao dung ngay
        group by sf_tmp.staff_id
      )
      , rpt_month as (
        select sf_tmp.staff_id, count(distinct trunc(rpt.check_month)) as tt_day 
        from sf_tmp
        join PRICE_CHECK_MONTH rpt
        on sf_tmp.staff_id = rpt.staff_id
        where 1 = 1
          and rpt.check_week is null
          and rpt.check_month >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and rpt.check_month  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1
          and trunc(rpt.create_date) >= to_date(''' || to_char(vv_bcycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'')
          and trunc(rpt.create_date)  < to_date(''' || to_char(vv_ecycle_date, 'yyyy-mm-dd') || ''', ''yyyy-mm-dd'') + 1 -- bao cao dung ngay
        group by sf_tmp.staff_id
      )
      , gain_tmp as (
        select sf_tmp.staff_id
          , nvl(rpt_day.tt_day, 0) + nvl(rpt_week.tt_day, 0) + nvl(rpt_month.tt_day, 0) as tt_report_day
        from sf_tmp
        left join rpt_day
        on sf_tmp.staff_id = rpt_day.staff_id
        left join rpt_week
        on sf_tmp.staff_id = rpt_week.staff_id
        left join rpt_month
        on sf_tmp.staff_id = rpt_month.staff_id
      )
      select sf.staff_id as staff_id, sf.shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , kqa.plan_value as plan_value
          , round(nvl(gain_tmp.tt_report_day, 0) * 100/ nullif(plan_tmp.tt_wkday, 0), 2) as gain
      from KPI_REGISTER_HIS vkrr
      join sf_tmp sf 
      on 1 = 1 ' ||
      (case when i_object_type = 2 then 
              ' and sf.staff_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' '
            when i_object_type = 4 then 
              ' and sf.istaff_type_id = vkrr.object_id and vkrr.object_id = ' || i_object_id || ' 
                and sf.staff_id not in (
                      select krhs.object_id
                      from KPI_REGISTER_HIS krhs
                      where krhs.kpi_period = 1
                        and krhs.kpi_period_value = vkrr.kpi_period_value
                        and krhs.object_type = 2 -- NV
                        and krhs.kpi_group_config_id is not null)'
       end) ||
      ' join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 2
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa on kqa.object_id = sf.staff_id and kqa.kpi_config_id = vkrr.kpi_config_id
      join plan_tmp  
      on plan_tmp.staff_id = sf.staff_id
      left join gain_tmp 
      on sf.staff_id = gain_tmp.staff_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type || '
        and kqa.plan_value is not null ';

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 1 -- nhan vien
              and rpt.object_id = v_dta(indx).staff_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          v_score := PKG_KPI_CYCLE.F_CAL_KPI_SCORE (
              v_dta(indx).plan_value
            , v_dta(indx).gain
            , v_dta(indx).weighted
            , i_max_value
            , v_param
          );
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    plan                = v_dta(indx).plan_value,
                   done                = v_dta(indx).gain,
                   score               = v_score,
                   weighted            = v_dta(indx).weighted,
                   max_value           = i_max_value,
                   update_date         = sysdate,
                   update_user         = 'SYS'
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
          elsif v_error_type = 1 
            and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             1,
             v_dta(indx).staff_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             v_dta(indx).plan_value,
             v_dta(indx).gain,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;staff_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).staff_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  /*exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));*/
  end P_KPI_REPORT_STAFF_CYCLE;
  
  PROCEDURE P_KPI_PO_OUTOF_SHOP_CYCLE (
    i_object_type number,
    i_object_id number,
    i_kpi_period_value number,
    i_kpi_group_config_id number,
    i_kpi_config_id number,
    i_kpi_config_code varchar2,
    i_plan_type number,
    i_kpi_reg_id number,
    i_max_value number,
    i_input_date date
  )
  /*
    @Procedure t?ng h?p KPI li�n quan [Qu� h?n, gia h?n giao nh?n; npp; chu k?];
    @author: thuattq1
    
    @params:  
    i_object_type         : Lo?i ??i t??ng: 1: NPP c? th?.
    i_object_id           : ID NPP.
    i_kpi_period_value    : ID gi� tr? k?.
    i_kpi_group_config_id : ID nh�m KPI.
    i_kpi_config_id       : ID KPI.
    i_kpi_config_code     : M� KPI.
    i_plan_type           : lo?i ph�n b?: 1: ko ph�n b?;
    i_kpi_reg_id          : ID KPI_REGISTER.
    i_max_value           : Gi� tr? tr?n (max ??t ???c).
  */
  as  
    v_sql clob; 
    v_group_column varchar2(100);
    v_rpt_id number(20);
    v_error_type number(2);
    v_error_msg nvarchar2(2000);
    v_params nvarchar2(2000);
    v_score RPT_KPI_CYCLE.SCORE%TYPE;
    v_sub_score RPT_KPI_CYCLE.SCORE%TYPE;
    v_percent number(2);
    
    v_weighted number;
    --v_atual_column  varchar2(50);
    v_count_param   number;
    v_cycle_edate CYCLE.end_date%type;
    
    SUB_SCORE     constant number(2) := 5; -- s? ?i?m tr? khi qu� h?n(?vt: ?i?m)
    PERCENT_APPLY constant number(2) := 5; -- quota b?t ??u tr? ?i?m (?vt: %)
    
    v_pro_name      constant varchar2(200) := 'P_KPI_PO_OUTOF_SHOP_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    TYPE curTyp IS REF CURSOR;
    c_dta curTyp;
    
    TYPE typ_dta IS RECORD (
      shop_id         SHOP.SHOP_ID%TYPE,
      weighted        KPI_GROUP_DETAIL.WEIGHTED%TYPE,
      max_value       KPI_GROUP_DETAIL.MAX_VALUE%TYPE,
      plan_type       NUMBER(2),
      plan_value      RPT_KPI_CYCLE.PLAN%TYPE,
      gain            RPT_KPI_CYCLE.DONE%TYPE
    );

    TYPE tab_dta IS TABLE OF typ_dta;
    v_dta tab_dta;   
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ''.,'' ';
    
    v_p_start_time := sysdate;
    v_p_params := '[i_object_type;i_object_id;i_kpi_period_value
      ;i_kpi_group_config_id;i_kpi_config_id;i_kpi_config_code
      ;i_plan_type;i_kpi_reg_id;i_max_value
      ;i_input_date]=['||
      i_object_type||';'|| i_object_id||';'|| i_kpi_period_value
      ||';'|| i_kpi_group_config_id||';'|| i_kpi_config_id||';'|| i_kpi_config_code
      ||';'|| i_plan_type||';'|| i_kpi_reg_id||';'|| i_max_value
      ||';'|| i_input_date||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    

    if  i_object_type is null
      or i_object_id is null
      or i_kpi_period_value is null
      or i_kpi_group_config_id is null
      or i_kpi_config_id is null
      or i_kpi_config_code is null
      or i_plan_type is null
    then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Input param is null');
      return;
    end if;
    
    if i_plan_type not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_plan_type not in [1]');
      return;
    end if;
    
    if i_object_type not in (1) then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'i_object_type not in [1]');
      return;
    end if;
    
    begin
      select trunc(ce.end_date)
      into v_cycle_edate
      from CYCLE ce
      where ce.cycle_id = i_kpi_period_value;
    exception
    when no_data_found then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'can''t find cycle with cycle_id = ' || i_kpi_period_value);
      return;
    when others then
      insert_log_procedure(v_pro_name, NULL, NULL, 3, 'error when get cycle info with cycle_id =  ' || i_kpi_period_value 
          || '. Exception: ' || SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000));
      return;
    end;
        
    if i_kpi_config_code in ('PO_OUTOF_SHOP_PRODUCT_GROUP') then 
        -- v_group_column := 'volumn';
        
        select count(1)
        into v_count_param
        from KPI_PARAM_VALUE kpve
        join KPI_PARAM kpm 
        on kpm.kpi_param_id = kpve.kpi_param_id 
          and kpm.status = 1 and kpm.type = 13
        where kpve.kpi_config_id =  i_kpi_config_id 
          and kpve.status in (0, 1)
          and kpve.from_kpi_period_value <= i_kpi_period_value 
          and (kpve.to_kpi_period_value >= i_kpi_period_value or kpve.to_kpi_period_value is null)
        ;
        
        if v_count_param > 0  then 
          v_params := 'product_id in (
              select kpgdl.product_id
              from KPI_PRODUCT_GROUP kpgp
              join KPI_PRODUCT_GROUP_DTL kpgdl
              on kpgdl.kpi_product_group_id = kpgp.kpi_product_group_id
                and kpgdl.status = 1
                and kpgdl.from_kpi_period_value <= ' || i_kpi_period_value || '
                and (kpgdl.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpgdl.to_kpi_period_value is null)
              where kpgp.status = 1
                and exists (
                  select 1
                  from KPI_PARAM_VALUE kpve
                  join KPI_PARAM kpm 
                  on kpm.kpi_param_id = kpve.kpi_param_id 
                    and kpm.status = 1 and kpm.type = 13
                  where kpve.kpi_config_id = ' || i_kpi_config_id || '
                    and kpve.status in (0, 1)
                    and kpve.from_kpi_period_value <= ' || i_kpi_period_value || '
                    and (kpve.to_kpi_period_value >= ' || i_kpi_period_value || ' or kpve.to_kpi_period_value is null)
                    and kpgp.code = pvl.value
                )
            ) ';
        end if;
        
    elsif i_kpi_config_code in ('PO_OUTOF_SHOP_ALL') then
        -- CHAY FULL
        v_group_column := '-1';
    else
        -- CHAY FULL
        v_group_column := '-1';
    end if;  
    
    v_sql := 
     'select sp.shop_id as shop_id, nvl(kqa.weighted, vkrr.weighted) as weighted, vkrr.max_value
          , vkrr.plan_type
          , spn.plan as plan_value
          , rpt.gain
      from KPI_REGISTER_HIS vkrr
      join SHOP sp 
      on sp.status = 1 
        and sp.shop_id = vkrr.object_id and sp.shop_id = ' || i_object_id ||'
      join (
        -- san luong ke hoach tu phu luc hop dong
        select po.shop_id --, pt.product_id, pdl.price
          , sum(nvl(pdl.quantity, 0)) as plan
        from PO
        join PO_DETAIL pdl
        on po.po_id = pdl.po_id
        join PRODUCT pt
        on pdl.product_id = pt.product_id
          and pt.status = 1
        where po.status = 1
          and po.po_type = 1
          and po.to_payment_date >= to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'')
          and po.to_payment_date <  to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
          and po.shop_id = ' || i_object_id || '
          '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
        group by po.shop_id
      ) spn 
      on spn.object_id = sp.shop_id 
      join (
          select kqat.object_type, kqat.object_id, kqat.kpi_config_id, kqat.weighted, kqat.plan_value
          from KPI_QUOTA kqat
          where kqat.kpi_period_value = ' || i_kpi_period_value || '
              and kqat.status         = 1
              and kqat.weighted is not null
              and kqat.object_type    = 1
              and kqat.kpi_register_id = ' || nvl(i_kpi_reg_id, -1) ||'
              and kqat.kpi_config_id = ' || i_kpi_config_id || '
      ) kqa 
      on kqa.object_id = sp.shop_id and kqa.kpi_config_id = vkrr.kpi_config_id
      left join (
        -- tong nhap tu cong ty
        select p.shop_id --, pd.product_id
          , sum(nvl(pd.quantity_received, 0)) as gain
        from PO_VNM p, PO_VNM_DETAIL_RECEIVED pd, PRODUCT pt
        where p.po_vnm_id = pd.po_vnm_id
            and pt.product_id = pd.product_id
            and pt.status = 1
            and p.type = 2 and p.status in (1, 2)
            and pd.import_date >= to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'')
            and pd.import_date <  to_date('''||to_char(v_cycle_edate, 'yyyymmdd')||''', ''yyyymmdd'') + 1
            and po.shop_id = ' || i_object_id || '
            '|| (case when trim(v_params) is not null then ' and pt.' || v_params else null end) ||' 
        group by p.shop_id
      ) rpt
      on sp.shop_id = rpt.shop_id
      where vkrr.kpi_period = 1 -- chu ky
        and vkrr.kpi_period_value = ' || i_kpi_period_value || '
        and vkrr.object_type = ' || i_object_type || '
        and vkrr.kpi_config_id = ' || i_kpi_config_id || '
        and vkrr.kpi_group_config_id = ' || i_kpi_group_config_id || '
        and vkrr.plan_type = ' || i_plan_type
        /*|| (case when i_plan_type = 2 then 'and kqa.plan_value is not null ' 
              when i_plan_type = 1 then 'and spn.plan is not null ' 
              else null end)*/
    ;

    dbms_output.put_line(v_sql);
    
    OPEN c_dta FOR v_sql; -- using i_object_type, i_kpi_group_config_id;
    LOOP
        FETCH c_dta 
        BULK COLLECT INTO v_dta LIMIT 500;

        FOR indx IN 1 .. v_dta.COUNT 
        LOOP
          begin
            select rpt_kpi_cycle_id
            into v_rpt_id
            from RPT_KPI_CYCLE rpt
            where rpt.cycle_id = i_kpi_period_value
              and rpt.shop_id = v_dta(indx).shop_id
              and rpt.kpi_group_config_id = i_kpi_group_config_id
              and kpi_config_id = i_kpi_config_id
              and kpi_register_id = i_kpi_reg_id
              and rpt.object_type = 3 -- npp
              and rpt.object_id = v_dta(indx).shop_id
            ;
                
            v_error_type := 0;
          exception
          when no_data_found then
            v_error_type := 1;
          when too_many_rows then
            v_error_type := 2;
          when others then
            v_error_type := 3;
            v_error_msg  := SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1000);
          end;
          
          -- t�nh % ko k?p nh?n.
          if nvl(v_dta(indx).plan_value, 0) = 0 
              or nvl(v_dta(indx).gain, 0) - nvl(v_dta(indx).plan_value, 0) > 0
          then
            -- neu so luong nhap > so luong phu luc hop dong -> ko co diem tru
            v_percent := 0;
          else
            v_percent := (v_dta(indx).plan_value - v_dta(indx).gain) * 100 / v_dta(indx).plan_value;
          end if;
          
          -- t�nh s? ?i?m tr?.
          v_sub_score := trunc(v_percent / PERCENT_APPLY) * SUB_SCORE;
          
          -- t�nh ?i?m c�n l?i = (s? ?i?m ???c nh?n t?i ?a) - (s? ?i?m b? tr?)
          v_score := (v_dta(indx).weighted / 100) - v_sub_score;
          
          if v_score < 0 then
            v_score := 0;
          end if;
          
          if v_error_type = 0 then
            update RPT_KPI_CYCLE
            set    -- plan         = v_dta(indx).plan_value,
                   done         = v_percent,
                   score        = v_score,
                   weighted     = v_dta(indx).weighted,
                   max_value    = i_max_value,
                   update_date  = sysdate,
                   update_user  = 'SYS'
            where  rpt_kpi_cycle_id = v_rpt_id
            ;
          elsif v_error_type = 1 
            -- and v_dta(indx).plan_value is not null -- chi cho phep insert dong co KHTT 
          then
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             null,
             v_percent,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value,
             sysdate,
             'SYS');
          elsif v_error_type = 2 then
            delete RPT_KPI_CYCLE
            where  rpt_kpi_cycle_id   = v_rpt_id
            -- RETURNING RPT_KPI_CYCLE
            -- BULK COLLECT INTO e_ids, d_ids
            ;
            
            insert into RPT_KPI_CYCLE (
               rpt_kpi_cycle_id, cycle_id, shop_id, 
               kpi_group_config_id, object_type, object_id, kpi_config_id, kpi_register_id,
               plan, done, 
               score, weighted,
               max_value, create_date, create_user) 
            values ( rpt_kpi_cycle_seq.nextval,
             i_kpi_period_value,
             v_dta(indx).shop_Id,
             i_kpi_group_config_id,
             3,
             v_dta(indx).shop_id,
             i_kpi_config_id,
             i_kpi_reg_id,
             null,
             v_percent,
             v_score, 
             v_dta(indx).weighted, 
             i_max_value, 
             sysdate,
             'SYS');
          elsif v_error_type = 3 then
            insert_log_procedure(v_pro_name, NULL, NULL, 3, 'Error: [cycle_id;shop_id;shop_id;kpi_group_config_id;kpi_config_id]=['
              ||i_kpi_period_value || ';'|| v_dta(indx).shop_id ||';'||v_dta(indx).shop_id||';'||i_kpi_group_config_id||';'||i_kpi_config_id||']. Exception: '
              ||v_error_msg);
          end if;
        END LOOP;

        EXIT WHEN v_dta.COUNT = 0; 
    END LOOP;
    CLOSE c_dta;
    
    COMMIT;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
  /*exception
  when others then
    rollback;
    insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1, 1500));*/
  end P_KPI_PO_OUTOF_SHOP_CYCLE;
  
  PROCEDURE P_RUN_KPI_CYCLE (
      i_input_date date default trunc(sysdate)
    , i_update_kpi_register number default 1
  )
  as
    v_pro_name      constant varchar2(200) := 'P_RUN_KPI_CYCLE';
    v_p_start_time  DATE;
    v_p_params      nvarchar2(2000);
    v_p_log_id      number;
    
    -- v_cycle_id CYCLE.CYCLE_ID%TYPE;
    
    vv_run_date date;
    vv_kpi_date CYCLE.end_date%type;
    v_cycle_id CYCLE.CYCLE_ID%TYPE;
    vv_bccycle_date CYCLE.end_date%type; -- ngay dau chu ky hien tai
    vv_eccycle_date CYCLE.end_date%type; -- ngay cuoi chu ky hien tai
    
    -- lay danh sach cac nhom KPI + object dang ky.
    cursor c_kpi_group is
    select vkrr.kpi_register_id, vkrr.kpi_group_config_id
      , vkrr.kpi_group_code as group_code
      , vkrr.object_type, vkrr.object_id
    from KPI_REGISTER_HIS vkrr
    where vkrr.kpi_period = 1 -- chu ky
      and vkrr.kpi_period_value = v_cycle_id
    group by vkrr.kpi_register_id, vkrr.kpi_group_config_id
      , vkrr.kpi_group_code
      , vkrr.object_type, vkrr.object_id
    order by vkrr.kpi_group_config_id, vkrr.kpi_register_id
      , vkrr.object_type, vkrr.object_id
    ;
    
    -- lay sanh sach kpi can chay tong hop
    cursor c_kpi (vv_kpi_register_id number, vv_kpi_group_config_id number, vv_object_type number, vv_object_id number) is
    select vkrr.object_type,
      vkrr.object_id,
      vkrr.kpi_period,
      vkrr.kpi_group_config_id,
      vkrr.kpi_config_id,
      vkrr.kpi_type_code as code,
      vkrr.procedure_code,
      vkrr.plan_type,
      vkrr.max_value
    from KPI_REGISTER_HIS vkrr
    where vkrr.kpi_register_id = vv_kpi_register_id
      and vkrr.kpi_group_config_id = vv_kpi_group_config_id
      and vkrr.object_type = vv_object_type
      and vkrr.object_id = vv_object_id
      and vkrr.kpi_period = 1 -- chu ky
      and vkrr.kpi_period_value = v_cycle_id
    ;
  begin
    v_p_start_time := sysdate;
    v_p_params := '[i_input_date;i_update_kpi_register]=['||
      i_input_date||';'|| i_update_kpi_register||']';
    
    v_p_log_id := F_INSERT_LOG_REPORT (v_pro_name, v_p_start_time, null, v_p_params);
    -- v_cycle_id  := f_get_cycle_seed(0);
    
    -- lay chu ky hien tai.
    vv_run_date := sysdate;
    select ce.cycle_id, trunc(ce.begin_date), trunc(ce.end_date)
    into v_cycle_id, vv_bccycle_date, vv_eccycle_date
    from CYCLE ce
    where ce.status = 1
      and ce.begin_date < trunc(vv_run_date) + 1
      and ce.end_date >= trunc(vv_run_date);
      
    if (  i_input_date >= vv_bccycle_date
      and i_input_date < vv_eccycle_date + 1
    ) then
      vv_kpi_date := trunc(vv_run_date);
    else
      select ce.cycle_id, trunc(ce.end_date)
      into v_cycle_id, vv_kpi_date
      from CYCLE ce
      where ce.status = 1
        and ce.begin_date < trunc(i_input_date) + 1
        and ce.end_date >= trunc(i_input_date);
    end if;
    
    -- c?p nh?t ??i t??ng ?k KPI theo d? li?u DB.
    -- SOS: tr??ng h?p ch?y cho qu� kh? c� th? ko c?p nh?t.
    if (i_update_kpi_register = 1) then
      P_KPI_REGISTER_HIS_FCYCLE (vv_kpi_date);
    end if;
    
    for v_kpi_group in c_kpi_group
    loop
      -- tong hop cho tung KPI con trong group
      dbms_output.put_line('run for group: ' || v_kpi_group.group_code);
      for v_kpi in c_kpi(v_kpi_group.kpi_register_id, v_kpi_group.kpi_group_config_id, v_kpi_group.object_type, v_kpi_group.object_id)
      loop
          /*P_KPI_SALE_PLAN_SHOP_CYCLE
          P_KPI_PO_RECEIVE_SHOP_CYCLE
          P_KPI_AMOUNT_STAFF_CYCLE
          P_KPI_CUS_VISIT_STAFF_CYCLE
          P_KPI_REPORT_STAFF_CYCLE*/
      
        dbms_output.put_line('run procedure: ' || v_kpi.procedure_code);
        if v_kpi.procedure_code = 'P_KPI_SALE_PLAN_SHOP_CYCLE' then
            -- KPI li�n quan [xxx ; nh�n vi�n; chu k?]
            dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
            P_KPI_SALE_PLAN_SHOP_CYCLE ( 
                v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
                v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
                v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
                v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_PO_RECEIVE_SHOP_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_PO_RECEIVE_SHOP_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_PO_OUTOF_SHOP_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_PO_OUTOF_SHOP_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_AMOUNT_STAFF_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_AMOUNT_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_CUS_VISIT_STAFF_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_CUS_VISIT_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        elsif v_kpi.procedure_code = 'P_KPI_REPORT_STAFF_CYCLE' then
          dbms_output.put_line('execute: ' || v_kpi.procedure_code || ' for kpi: ' || v_kpi.code);
          P_KPI_REPORT_STAFF_CYCLE ( 
              v_kpi.OBJECT_TYPE, v_kpi.OBJECT_ID, v_cycle_id, 
              v_kpi.KPI_GROUP_CONFIG_ID, v_kpi.KPI_CONFIG_ID, v_kpi.code, 
              v_kpi.PLAN_TYPE, v_kpi_group.kpi_register_id, 
              v_kpi.MAX_VALUE, vv_kpi_date );
        
        else
          dbms_output.put_line('not execute: ' || v_kpi.procedure_code);
        end if;
      end loop;
    end loop;
    
    commit;
    
    P_UPDATE_END_TIME_LOG_REPORT (v_p_log_id, null);
    <<end_procedure>>
    rollback;
    
  exception
  when others then
    rollback;
  -- insert_log_procedure(v_pro_name, NULL, NULL, 3, SQLCODE || ' : ' || SUBSTR (sqlerrm, 1500, 1500));
  -- raise;
  end P_RUN_KPI_CYCLE;
END PKG_KPI_CYCLE;